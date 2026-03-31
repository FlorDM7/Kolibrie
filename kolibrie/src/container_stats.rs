/**
 * Collect some stats from a content container which can represent a window during SP.
 * These stats include total amount of triples and some cardinalities.
 * This is inspired by the DatabaseStats struct used in the streamertail_optimizer.
 */
use std::collections::{HashMap, HashSet};
use shared::triple::Triple;
use crate::rsp::s2r::*;

#[derive(Debug, Clone)]
pub struct ContainerStats {
    pub total_triples: i64,
    pub predicate_cardinalities: HashMap<u32, i64>, // meaning u32 appears a number of i64 times in the container
    pub subject_cardinalities: HashMap<u32, i64>,
    pub object_cardinalities: HashMap<u32, i64>,
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
        let total_triples: i64 = container.len().try_into().unwrap();
        // Build cardinality maps
        let mut predicate_cardinalities: HashMap<u32, i64> = HashMap::new();
        let mut subject_cardinalities: HashMap<u32, i64> = HashMap::new();
        let mut object_cardinalities: HashMap<u32, i64> = HashMap::new();

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

    // Get the total amount of triples
    pub fn get_total_triples(&self) -> i64 {
        self.total_triples
    }

    // Gets the amount of unique objects
    pub fn get_total_objects(&self) -> usize {
        self.object_cardinalities.keys().len()
    }

    // Gets the amount of unique predicates
    pub fn get_total_predicates(&self) -> usize {
        self.predicate_cardinalities.keys().len()
    }

    // Gets the amount of unique subjects
    pub fn get_total_subjects(&self) -> usize {
        self.subject_cardinalities.keys().len()
    }

    // Gets the cardinality for an object
    pub fn get_object_cardinality(&self, object: u32) -> i64 {
        self.object_cardinalities.get(&object).copied().unwrap_or(0)
    }

    // Gets the cardinality for a predicate
    pub fn get_predicate_cardinality(&self, predicate: u32) -> i64 {
        self.predicate_cardinalities.get(&predicate).copied().unwrap_or(0)
    }

    // vibe
    // Gets or computes join selectivity for a predicate.
    // Follows the same heuristic as DatabaseStats:
    // selectivity = predicate_cardinality / total_triples.
    pub fn get_join_selectivity(&self, predicate: u32) -> f64 {
        let cardinality = self.get_predicate_cardinality(predicate);
        if self.total_triples > 0 {
            (cardinality as f64) / (self.total_triples as f64)
        } else {
            0.1
        }
    }

    // Gets the cardinality for an subject
    pub fn get_subject_cardinality(&self, subject: u32) -> i64 {
        self.subject_cardinalities.get(&subject).copied().unwrap_or(0)
    }

    // Updates statistics with new data
    pub fn update_stats(&mut self, subject: u32, predicate: u32, object: u32) {
        self.total_triples += 1;
        *self.predicate_cardinalities.entry(predicate).or_insert(0) += 1;
        *self.subject_cardinalities.entry(subject).or_insert(0) += 1;
        *self.object_cardinalities.entry(object).or_insert(0) += 1;
    }

    /*
    Calculate the difference of two hashmaps
    For example a = {1: 3, 2: 10} and b = {1: 2, 3: 5}
    Then result = {1: 1, 2: 10, 3: -5}
    */
    fn diff_of_hashmaps(a: &HashMap<u32, i64>, b: &HashMap<u32, i64>) -> HashMap<u32, i64> {
    let all_keys: HashSet<_> = a.keys().chain(b.keys()).collect(); // Get all unique keys from both maps
    all_keys
        .into_iter()
        .map(|&key| {
            let val_a = a.get(&key).copied().unwrap_or(0);
            let val_b = b.get(&key).copied().unwrap_or(0);
            (key, val_a - val_b)
        })
        .collect()
    }

    // result is basically self - previousStats for all elements
    pub fn compare(&mut self, previous_stats: ContainerStats) -> Self {
        let total_triples = self.get_total_triples() - previous_stats.get_total_triples();

        let predicate_cardinalities = Self::diff_of_hashmaps(&self.predicate_cardinalities, &previous_stats.predicate_cardinalities);
        let subject_cardinalities = Self::diff_of_hashmaps(&self.subject_cardinalities, &previous_stats.subject_cardinalities);
        let object_cardinalities = Self::diff_of_hashmaps(&self.object_cardinalities, &previous_stats.object_cardinalities);

        // return stats struct
        Self {
            total_triples, // difference in the amount of triples from current windows versus previous window
            predicate_cardinalities,
            subject_cardinalities,
            object_cardinalities,
        }
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
        assert_eq!(stats.get_total_objects(), 0);
        assert_eq!(stats.get_total_predicates(), 0);
        assert_eq!(stats.get_total_subjects(), 0);
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
        assert_eq!(stats.get_total_objects(), 3);
        assert_eq!(stats.get_total_predicates(), 2);
        assert_eq!(stats.get_total_subjects(), 2);

        assert_eq!(stats.get_object_cardinality(5), 1);
        assert_eq!(stats.get_object_cardinality(6), 1);
        assert_eq!(stats.get_object_cardinality(7), 1);
        assert_eq!(stats.get_predicate_cardinality(1), 1);
        assert_eq!(stats.get_predicate_cardinality(2), 2);
        assert_eq!(stats.get_subject_cardinality(10), 1);
        assert_eq!(stats.get_subject_cardinality(11), 2);
    }

    #[test]
    fn test_compare_function() {
        // previous window
        let triples = vec![
            Triple {
                subject: 1,
                predicate: 11,
                object: 103 
            },
            Triple {
                subject: 1,
                predicate: 10,
                object: 101,
            },
            Triple {
                subject: 2,
                predicate: 11,
                object: 100,
            }
        ];

        let container = ContentContainer::from_items(triples, 1);
        let previous_stats = ContainerStats::gather_stats(&container);
        
        // current window 
        let triples = vec![
            Triple {
                subject: 1,
                predicate: 10,
                object: 100 
            },
            Triple {
                subject: 2,
                predicate: 10,
                object: 100,
            },
            Triple {
                subject: 3,
                predicate: 10,
                object: 101,
            },
            Triple {
                subject: 3,
                predicate: 11,
                object: 102,
            }
        ];

        let container = ContentContainer::from_items(triples, 1);
        let mut current_stats = ContainerStats::gather_stats(&container);

        let diff_stats = current_stats.compare(previous_stats);

        assert_eq!(diff_stats.get_total_triples(), 1); // 4 - 3
        
        assert_eq!(diff_stats.get_subject_cardinality(1), -1);
        assert_eq!(diff_stats.get_subject_cardinality(2), 0);
        assert_eq!(diff_stats.get_subject_cardinality(3), 2);

        assert_eq!(diff_stats.get_predicate_cardinality(10), 2);
        assert_eq!(diff_stats.get_predicate_cardinality(11), -1);

        assert_eq!(diff_stats.get_object_cardinality(100), 1);
        assert_eq!(diff_stats.get_object_cardinality(101), 0);
        assert_eq!(diff_stats.get_object_cardinality(102), 1);
        assert_eq!(diff_stats.get_object_cardinality(103), -1);
    }

    // vibe
    #[test]
    fn test_get_join_selectivity() {
        let triples = vec![
            Triple {
                subject: 10,
                predicate: 2,
                object: 5,
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
            },
        ];

        let container = ContentContainer::from_items(triples, 1);
        let stats = ContainerStats::gather_stats(&container);

        // predicate 2 appears 2 times over 3 triples
        assert!((stats.get_join_selectivity(2) - (2.0 / 3.0)).abs() < 1e-9);

        // unknown predicate has 0 cardinality
        assert_eq!(stats.get_join_selectivity(999), 0.0);
    }
}
