pub type Name = u64;

pub trait Lineage {
    // query
    fn dependencies(&self, name: Name) -> Vec<Name>;
    fn dependents(&self, name: Name) -> Vec<Name>;
    fn dependencies_all(&self, name: Name, k: usize) -> Vec<Name>;
    fn dependents_all(&self, name: Name, k: usize) -> Vec<Name>;
    // update
    fn upsert(&self, name: Name, dependencies: Vec<Name>);
    fn delete(&self, name: Name);
}
