use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use differential_dataflow::operators::arrange::{upsert, ArrangeByKey, TraceAgent};
use differential_dataflow::operators::{Iterate, Join, Reduce};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::AsCollection;
use timely::communication::Allocate;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::{Input, ToStream};
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::progress::frontier::AntichainRef;
use timely::worker::Worker;

use crate::lineage::{Lineage, Name};

struct Differential {
    tx: Sender<Message>,
    _thread: Option<JoinHandle<()>>,
}

pub fn new() -> Arc<dyn Lineage> {
    let (tx, rx) = unbounded();
    let thread = std::thread::spawn(move || run(rx));
    Arc::new(Differential {
        tx,
        _thread: Some(thread),
    })
}

impl Lineage for Differential {
    fn dependencies(&self, name: Name) -> Vec<Name> {
        let (tx, rx) = bounded(1);
        let req = Message::Dependencies { name, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn dependents(&self, name: Name) -> Vec<Name> {
        let (tx, rx) = bounded(1);
        let req = Message::Dependents { name, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn dependencies_cascade(&self, name: Name) -> HashMap<Name, Vec<Name>> {
        let (tx, rx) = bounded(1);
        let req = Message::DependenciesCascade { name, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn dependents_cascade(&self, name: Name) -> HashMap<Name, Vec<Name>> {
        let (tx, rx) = bounded(1);
        let req = Message::DependentsCascade { name, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn dependencies_k(&self, name: Name, k: usize) -> HashMap<Name, Vec<Name>> {
        let (tx, rx) = bounded(1);
        let req = Message::DependenciesK { name, k, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn dependents_k(&self, name: Name, k: usize) -> HashMap<Name, Vec<Name>> {
        let (tx, rx) = bounded(1);
        let req = Message::DependentsK { name, k, tx };
        self.tx.send(req).unwrap();
        rx.recv().unwrap()
    }

    fn upsert(&self, name: Name, dependencies: Vec<Name>) {
        let req = Message::Upsert { name, dependencies };
        self.tx.send(req).unwrap();
    }

    fn delete(&self, name: Name) {
        let req = Message::Delete { name };
        self.tx.send(req).unwrap();
    }
}

enum Message {
    Dependencies {
        name: Name,
        tx: Sender<Vec<Name>>,
    },
    Dependents {
        name: Name,
        tx: Sender<Vec<Name>>,
    },
    DependenciesCascade {
        name: Name,
        tx: Sender<HashMap<Name, Vec<Name>>>,
    },
    DependentsCascade {
        name: Name,
        tx: Sender<HashMap<Name, Vec<Name>>>,
    },
    DependenciesK {
        name: Name,
        k: usize,
        tx: Sender<HashMap<Name, Vec<Name>>>,
    },
    DependentsK {
        name: Name,
        k: usize,
        tx: Sender<HashMap<Name, Vec<Name>>>,
    },
    Upsert {
        name: Name,
        dependencies: Vec<Name>,
    },
    Delete {
        name: Name,
    },
}

type Key = Name;
type Val = Name;
type ValVec = Vec<Name>;
type Timestamp = u64;
type Spine = OrdValSpine<Key, Val, Timestamp, isize>;
type TraceHandle = TraceAgent<Spine>;

struct Context {
    input: Handle<Timestamp, (Key, Option<ValVec>, Timestamp)>,
    counter: Timestamp,
    probe: ProbeHandle<Timestamp>,
}

impl Context {
    fn new() -> Self {
        let input: Handle<Timestamp, _> = InputHandle::new();
        let counter = *input.time();
        let probe = ProbeHandle::new();
        Context {
            input,
            counter,
            probe,
        }
    }

    fn advance<A: Allocate>(&mut self, traces: [&mut TraceHandle; 2], worker: &mut Worker<A>) {
        self.counter += 1;
        self.input.advance_to(self.counter);
        let frontier = &[self.counter];
        for trace in traces.into_iter() {
            (*trace).set_physical_compaction(AntichainRef::new(frontier));
            (*trace).set_logical_compaction(AntichainRef::new(frontier));
        }
        worker.step_while(|| self.probe.less_than(self.input.time()));
    }

    fn query<A: Allocate>(
        &mut self,
        trace: &mut TraceHandle,
        name: Name,
        worker: &mut Worker<A>,
    ) -> Vec<Val> {
        let current = self.counter;
        let mut result_trace = worker.dataflow(|scope| {
            let query = Some(name)
                .to_stream(scope)
                .map(move |x| (x, current, 1))
                .as_collection();
            let lineage = trace.import(scope).semijoin(&query).arrange_by_key();

            lineage.stream.probe_with(&mut self.probe);
            lineage.trace
        });

        self.advance([trace, &mut result_trace], worker);
        let mut result = self.read(&mut result_trace);
        result.pop().map(|d| d.1).unwrap_or(vec![])
    }

    fn query_cascade<A: Allocate>(
        &mut self,
        trace: &mut TraceHandle,
        name: Name,
        worker: &mut Worker<A>,
    ) -> HashMap<Key, Vec<Val>> {
        let current = self.counter;
        let mut result_trace = worker.dataflow(|scope| {
            let query = Some(name)
                .to_stream(scope)
                .map(move |x| (x, current, 1))
                .as_collection();
            let arranged = trace.import(scope);
            let init = arranged.semijoin(&query);
            let res = init
                .iterate(|lineage| {
                    let targets = lineage.map(|kv| kv.1);
                    arranged
                        .enter(&lineage.scope())
                        .semijoin(&targets)
                        .concat(lineage)
                        .reduce(|_key, input, output| {
                            for (v, _) in input {
                                output.push(((*v).clone(), 1));
                            }
                        })
                })
                .arrange_by_key();

            res.stream.probe_with(&mut self.probe);
            res.trace
        });

        self.advance([trace, &mut result_trace], worker);
        self.read(&mut result_trace).into_iter().collect()
    }

    fn query_k<A: Allocate>(
        &mut self,
        trace: &mut TraceHandle,
        name: Name,
        worker: &mut Worker<A>,
        k: usize,
    ) -> HashMap<Key, Vec<Val>> {
        if k == 0 {
            return HashMap::new();
        }

        let current = self.counter;
        let mut result_trace =
            worker.dataflow(|scope| {
                let query = Some(name)
                    .to_stream(scope)
                    .map(move |x| (x, current, 1))
                    .as_collection();
                let arranged = trace.import(scope);
                let mut lineage = arranged.semijoin(&query);
                for _ in 0..(k - 1) {
                    let targets = lineage.map(|kv| kv.1);
                    lineage = arranged.semijoin(&targets).concat(&lineage).reduce(
                        |_key, input, output| {
                            for (v, _) in input {
                                output.push(((*v).clone(), 1));
                            }
                        },
                    )
                }
                let res = lineage.arrange_by_key();
                res.stream.probe_with(&mut self.probe);
                res.trace
            });

        self.advance([trace, &mut result_trace], worker);
        self.read(&mut result_trace).into_iter().collect()
    }

    fn read(&self, trace: &mut TraceHandle) -> Vec<(Key, Vec<Val>)> {
        use timely::PartialOrder;

        let mut ret = vec![];
        let (mut cursor, storage) = trace.cursor();
        while cursor.key_valid(&storage) {
            let mut values = vec![];
            while cursor.val_valid(&storage) {
                let mut copies = 0;
                cursor.map_times(&storage, |time, diff| {
                    if time.less_equal(&self.counter) {
                        copies += diff;
                    }
                });
                if copies < 0 {
                    panic!(
                        "Negative multiplicity: {} for {:?}",
                        copies,
                        cursor.key(&storage)
                    );
                }
                for _ in 0..copies {
                    values.push(cursor.val(&storage).clone());
                }
                cursor.step_val(&storage);
            }
            if !values.is_empty() {
                ret.push((cursor.key(&storage).clone(), values));
            }
            cursor.step_key(&storage);
        }
        ret
    }
}

fn run(rx: Receiver<Message>) {
    timely::execute(timely::Config::thread(), move |worker| {
        let mut ctx = Context::new();
        let (mut upstream, mut downstream) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let stream = scope.input_from(&mut ctx.input);
            let arranged = upsert::arrange_from_upsert::<_, OrdValSpine<Key, ValVec, _, _>>(
                &stream, &"lineage",
            );

            let upstream = arranged
                .as_collection(|k, v| (k.clone(), v.clone()))
                .flat_map(|(k, vs)| vs.into_iter().map(move |v| (k, v)));
            let downstream = upstream.map(|(k, v)| (v, k));

            (
                upstream.arrange_by_key().trace,
                downstream.arrange_by_key().trace,
            )
        });

        loop {
            let message = match rx.recv() {
                Ok(d) => d,
                Err(_) => break,
            };
            match message {
                Message::Dependencies { name, tx } => {
                    let d = ctx.query(&mut upstream, name, worker);
                    tx.send(d).unwrap();
                }
                Message::Dependents { name, tx } => {
                    let d = ctx.query(&mut downstream, name, worker);
                    tx.send(d).unwrap();
                }
                Message::DependenciesCascade { name, tx } => {
                    let d = ctx.query_cascade(&mut upstream, name, worker);
                    tx.send(d).unwrap();
                }
                Message::DependentsCascade { name, tx } => {
                    let d = ctx.query_cascade(&mut downstream, name, worker);
                    tx.send(d).unwrap();
                }
                Message::DependenciesK { name, k, tx } => {
                    let d = ctx.query_k(&mut upstream, name, worker, k);
                    tx.send(d).unwrap();
                }
                Message::DependentsK { name, k, tx } => {
                    let d = ctx.query_k(&mut downstream, name, worker, k);
                    tx.send(d).unwrap();
                }
                Message::Upsert { name, dependencies } => {
                    ctx.input.send((name, Some(dependencies), ctx.counter))
                }
                Message::Delete { name } => ctx.input.send((name, None, ctx.counter)),
            }
        }
    })
    .unwrap();
}
