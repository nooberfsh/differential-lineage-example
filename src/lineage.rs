use std::collections::HashMap;

pub type Name = u64;

pub trait Lineage {
    // query
    fn dependencies(&self, name: Name) -> Vec<Name>;
    fn dependents(&self, name: Name) -> Vec<Name>;
    fn dependencies_cascade(&self, name: Name) -> HashMap<Name, Vec<Name>>;
    fn dependents_cascade(&self, name: Name) -> HashMap<Name, Vec<Name>>;
    // update
    fn upsert(&self, name: Name, dependencies: Vec<Name>);
    fn delete(&self, name: Name);
}
