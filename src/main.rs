pub mod differential;
pub mod lineage;

fn main() {
    let lineage = differential::new();
    lineage.upsert(1, vec![2, 3]);
    lineage.upsert(2, vec![4, 5]);
    println!("dependencies for {}: {:?}", 1, lineage.dependencies(1));
    println!("dependencies for {}: {:?}", 2, lineage.dependencies(2));
    println!("dependents for {}: {:?}", 2, lineage.dependents(2));
    println!("dependents for {}: {:?}", 5, lineage.dependents(5));

    println!(
        "dependencies for {}: {:?}",
        1,
        lineage.dependencies_cascade(1)
    );
}
