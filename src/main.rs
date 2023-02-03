pub mod differential;
pub mod lineage;

fn main() {
    let lineage = differential::new();
    lineage.upsert(1, vec![2, 3]);
    lineage.upsert(2, vec![4, 5]);
    lineage.upsert(0, vec![1, 3]);
    lineage.upsert(5, vec![6, 7, 8]);
    println!("dependencies for {}: {:?}", 1, lineage.dependencies(1));
    println!("dependencies for {}: {:?}", 2, lineage.dependencies(2));
    println!("dependents for {}: {:?}", 2, lineage.dependents(2));
    println!("dependents for {}: {:?}", 5, lineage.dependents(5));
    println!(
        "dependencies for {}: {:?}",
        1,
        lineage.dependencies_cascade(1)
    );
    println!("dependencies for {}: {:?}", 0, lineage.dependencies_k(0, 4));
    println!("dependents for {}: {:?}", 4, lineage.dependents_cascade(4));

    lineage.delete(0);
    println!("dependents for {}: {:?}", 4, lineage.dependents_cascade(4));
}
