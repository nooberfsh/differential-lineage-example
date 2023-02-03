use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use differential_dataflow::operators::arrange::{upsert, ArrangeByKey, TraceAgent};
use differential_dataflow::operators::Join;
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
    thread: Option<JoinHandle<()>>,
}

pub fn new() -> Arc<dyn Lineage> {
    let (tx, rx) = unbounded();
    let thread = std::thread::spawn(move || run(rx));
    Arc::new(Differential {
        tx,
        thread: Some(thread),
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

    fn dependencies_all(&self, name: Name, k: usize) -> Vec<Name> {
        todo!()
    }

    fn dependents_all(&self, name: Name, k: usize) -> Vec<Name> {
        todo!()
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
    Dependencies { name: Name, tx: Sender<Vec<Name>> },
    Dependents { name: Name, tx: Sender<Vec<Name>> },
    Upsert { name: Name, dependencies: Vec<Name> },
    Delete { name: Name },
}

type Key = Name;
type Val = Name;
type ValVec = Vec<Name>;
type Timestamp = u64;
type Spine = OrdValSpine<Key, Val, Timestamp, isize>;
type TraceHandle = TraceAgent<Spine>;

struct Config {}

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

    fn advance(&mut self) {
        self.counter += 1;
        self.input.advance_to(self.counter);
    }

    fn compact(&self, trace: &mut TraceHandle) {
        let frontier = &[self.counter];
        trace.set_physical_compaction(AntichainRef::new(frontier));
        trace.set_logical_compaction(AntichainRef::new(frontier));
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

        self.advance();
        self.compact(trace);
        self.compact(&mut result_trace);
        worker.step_while(|| self.probed());
        let mut result = self.read(&mut result_trace);
        result.pop().map(|d| d.1).unwrap_or(vec![])
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

    fn probed(&self) -> bool {
        self.probe.less_than(self.input.time())
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
                Message::Upsert { name, dependencies } => {
                    ctx.input.send((name, Some(dependencies), ctx.counter))
                }
                Message::Delete { name } => ctx.input.send((name, None, ctx.counter)),
            }
        }
    })
    .unwrap();
}
