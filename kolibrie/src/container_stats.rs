/**
 * Collect some stats from a content container which can represent a window during SP.
 * These stats include total amount of triples and some cardinalities.
 * This is inspired by the DatabaseStats struct used in the streamertail_optimizer.
 */
use std::collections::HashMap;
use shared::triple::Triple;
use crate::rsp::s2r::*;

#[derive(Debug)]
pub struct ContainerStats {
    pub total_triples: u64,
    pub predicate_cardinalities: HashMap<u32, u64>, // meaning u32 appears a number of u64 times in the container
    pub subject_cardinalities: HashMap<u32, u64>,
    pub object_cardinalities: HashMap<u32, u64>,
}

impl ContainerStats {
    // Creates new container stats instance
    pub fn new() -> Self {
        Self {
            total_triples: 0,
            predicate_cardinalities: HashMap::new(),
            subject_cardinalities: HashMap::new(),
            object_cardinalities: HashMap::new()
        }
    }

    // Gather stats for a given container
    pub fn gather_stats(container: &ContentContainer<Triple>) -> Self {
        let total_triples: u64 = container.len().try_into().unwrap();
        // Build cardinality maps
        let mut predicate_cardinalities: HashMap<u32, u64> = HashMap::new();
        let mut subject_cardinalities: HashMap<u32, u64> = HashMap::new();
        let mut object_cardinalities: HashMap<u32, u64> = HashMap::new();

        // iterate over the triples
        for triple in container.iter() {
            // calculate cardinality
            *predicate_cardinalities.entry(triple.predicate).or_insert(0) += 1;
            *subject_cardinalities.entry(triple.subject).or_insert(0) += 1;
            *object_cardinalities.entry(triple.object).or_insert(0) += 1;
        }
        
        // return stats struct
        Self {
            total_triples,
            predicate_cardinalities,
            subject_cardinalities,
            object_cardinalities,
        }
    }

    // Gets the cardinality for an object
    pub fn get_object_cardinality(&self, object: u32) -> u64 {
        self.object_cardinalities.get(&object).copied().unwrap_or(0)
    }

    // Gets the cardinality for a predicate
    pub fn get_predicate_cardinality(&self, predicate: u32) -> u64 {
        self.predicate_cardinalities.get(&predicate).copied().unwrap_or(0)
    }

    /// Gets the cardinality for an subject
    pub fn get_subject_cardinality(&self, subject: u32) -> u64 {
        self.subject_cardinalities.get(&subject).copied().unwrap_or(0)
    }

    // Updates statistics with new data
    pub fn update_stats(&mut self, subject: u32, predicate: u32, object: u32) {
        self.total_triples += 1;
        *self.predicate_cardinalities.entry(predicate).or_insert(0) += 1;
        *self.subject_cardinalities.entry(subject).or_insert(0) += 1;
        *self.object_cardinalities.entry(object).or_insert(0) += 1;
    }

}

// Implement the default trait
impl Default for ContainerStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_new_container() {
        let container = ContentContainer::from_items(Vec::new(), 1);
        let stats = ContainerStats::gather_stats(&container);

        // contains no triples
        assert_eq!(stats.total_triples, 0);
        assert_eq!(stats.object_cardinalities.keys().len(), 0);
        assert_eq!(stats.predicate_cardinalities.keys().len(), 0);
        assert_eq!(stats.subject_cardinalities.keys().len(), 0);
    }

    #[test]
    fn test_some_cardinalities() {
        let triples = vec![
            Triple {
                subject: 10,
                predicate: 2,
                object: 5 
            },
            Triple {
                subject: 11,
                predicate: 2,
                object: 6,
            },
            Triple {
                subject: 11,
                predicate: 1,
                object: 7,
            }
        ];

        let container = ContentContainer::from_items(triples, 1);
        let stats = ContainerStats::gather_stats(&container);
        
        assert_eq!(stats.total_triples, 3);
        assert_eq!(stats.object_cardinalities.keys().len(), 3);
        assert_eq!(stats.predicate_cardinalities.keys().len(), 2);
        assert_eq!(stats.subject_cardinalities.keys().len(), 2);

        assert_eq!(stats.get_object_cardinality(5), 1);
        assert_eq!(stats.get_object_cardinality(6), 1);
        assert_eq!(stats.get_object_cardinality(7), 1);
        assert_eq!(stats.get_predicate_cardinality(1), 1);
        assert_eq!(stats.get_predicate_cardinality(2), 2);
        assert_eq!(stats.get_subject_cardinality(10), 1);
        assert_eq!(stats.get_subject_cardinality(11), 2);
    }
}
