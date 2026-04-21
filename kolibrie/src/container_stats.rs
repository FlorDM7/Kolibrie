/**
 * Collect some stats from a content container which can represent a window during SP.
 * These stats include total amount of triples and some cardinalities.
 * This is inspired by the DatabaseStats struct used in the streamertail_optimizer.
 */
use std::{cmp::max, collections::{HashMap, HashSet}};
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

    /*
     Thresholds to decide if recalculating the plan is needed
     */

    // OPTION 1: WINDOW SIZE
    // If the size of the window changes with a "big" margin, return true
    // advantage: simple, quick
    // disadvantage: change also needed when window is of equal size
    pub fn size_change_ratio(&self, previous_stats: &ContainerStats) -> f64 {
        let current_amount = self.total_triples;
        let previous_amount = previous_stats.get_total_triples();
        let size_change = current_amount - previous_amount;
        let mut size_change = size_change.abs() as f64;
        size_change = size_change / (max(previous_amount, 1) as f64);
        size_change
    }

    pub fn should_replan_size_change(&self, previous_stats: &ContainerStats) -> bool {
        self.size_change_ratio(previous_stats) > 0.2
    }

    // OPTION 2: DIFFERENCE IN PREDICATE COUNT DISTRIBUTION
    //                         SUBJECT
    //                         OBJECT
    // advantages: capture statiscal drift
    // disadvantages: not directly if joins should change
    pub fn should_replan_predicate_distribution(&self, previous_stats: &ContainerStats) -> bool {
        // Total variation distance over predicate distributions.
        // 0.0 means identical distribution, 1.0 means maximum shift.
        let distance = self.predicate_distribution_distance(previous_stats);
        distance > 0.15
    }

    // Same as predicate distribution shift, but for subjects.
    pub fn should_replan_subject_distribution(&self, previous_stats: &ContainerStats) -> bool {
        self.subject_distribution_distance(previous_stats) > 0.15
    }

    // Same as predicate distribution shift, but for objects.
    pub fn should_replan_object_distribution(&self, previous_stats: &ContainerStats) -> bool {
        self.object_distribution_distance(previous_stats) > 0.15
    }

    // Computes total variation distance between predicate distributions.
    // L1 Total Variation
    pub fn predicate_distribution_distance(&self, previous_stats: &ContainerStats) -> f64 {
        Self::distribution_distance_from_maps(
            &self.predicate_cardinalities,
            &previous_stats.predicate_cardinalities,
        )
    }

    pub fn subject_distribution_distance(&self, previous_stats: &ContainerStats) -> f64 {
        Self::distribution_distance_from_maps(
            &self.subject_cardinalities,
            &previous_stats.subject_cardinalities,
        )
    }

    pub fn object_distribution_distance(&self, previous_stats: &ContainerStats) -> f64 {
        Self::distribution_distance_from_maps(
            &self.object_cardinalities,
            &previous_stats.object_cardinalities,
        )
    }

    fn distribution_distance_from_maps(current: &HashMap<u32, i64>, previous: &HashMap<u32, i64>) -> f64 {
        let mut all_keys: HashSet<u32> = current.keys().copied().collect();
        all_keys.extend(previous.keys().copied());

        let curr_total = max(current.values().copied().sum::<i64>(), 1) as f64;
        let prev_total = max(previous.values().copied().sum::<i64>(), 1) as f64;

        let l1_distance: f64 = all_keys
            .iter()
            .map(|key| {
                let curr_prob = (current.get(key).copied().unwrap_or(0) as f64) / curr_total;
                let prev_prob = (previous.get(key).copied().unwrap_or(0) as f64) / prev_total;
                (curr_prob - prev_prob).abs()
            })
            .sum();
        
        dbg!("L1 distance: {}", 0.5*l1_distance);
        0.5 * l1_distance
    }

    // OPTION 3: RANK-CHANGE TRIGGER
    // advantages: looks at join ordering changes
    // disadvantages: lot of work, naad stable handling for ties
    pub fn should_replan_rank_change(&self, previous_stats: &ContainerStats) -> bool {
        // Replan if enough pairwise ordering relations between predicates flipped.
        let rank_ratio = self.rank_change_ratio(previous_stats);
        rank_ratio > 0.2
    }

    // Same as predicate rank-change, but for subjects.
    pub fn should_replan_subject_rank_change(&self, previous_stats: &ContainerStats) -> bool {
        self.subject_rank_change_ratio(previous_stats) > 0.2
    }

    // Same as predicate rank-change, but for objects.
    pub fn should_replan_object_rank_change(&self, previous_stats: &ContainerStats) -> bool {
        self.object_rank_change_ratio(previous_stats) > 0.2
    }

    // Pairwise rank-change ratio across predicate selectivities.
    // A "pair" is any 2 predicates (pi, pj). A pair is "flipped"
    // when their relative order changes between windows.
    pub fn rank_change_ratio(&self, previous_stats: &ContainerStats) -> f64 {
        Self::rank_change_ratio_from_maps(
            &self.predicate_cardinalities,
            &previous_stats.predicate_cardinalities,
        )
    }

    fn subject_rank_change_ratio(&self, previous_stats: &ContainerStats) -> f64 {
        Self::rank_change_ratio_from_maps(
            &self.subject_cardinalities,
            &previous_stats.subject_cardinalities,
        )
    }

    fn object_rank_change_ratio(&self, previous_stats: &ContainerStats) -> f64 {
        Self::rank_change_ratio_from_maps(
            &self.object_cardinalities,
            &previous_stats.object_cardinalities,
        )
    }

    fn rank_change_ratio_from_maps(current: &HashMap<u32, i64>, previous: &HashMap<u32, i64>) -> f64 {
        let mut keys: Vec<u32> = current
            .keys()
            .chain(previous.keys())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        keys.sort_unstable();

        if keys.len() < 2 {
            return 0.0;
        }

        let current_total = max(current.values().copied().sum::<i64>(), 1) as f64;
        let previous_total = max(previous.values().copied().sum::<i64>(), 1) as f64;

        let mut flipped_pairs = 0usize;
        let mut comparable_pairs = 0usize;

        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                let ki = keys[i];
                let kj = keys[j];

                let prev_cmp = compare_with_epsilon(
                    (previous.get(&ki).copied().unwrap_or(0) as f64) / previous_total,
                    (previous.get(&kj).copied().unwrap_or(0) as f64) / previous_total,
                    1e-12,
                );
                let curr_cmp = compare_with_epsilon(
                    (current.get(&ki).copied().unwrap_or(0) as f64) / current_total,
                    (current.get(&kj).copied().unwrap_or(0) as f64) / current_total,
                    1e-12,
                );

                if prev_cmp == 0 || curr_cmp == 0 {
                    continue;
                }

                comparable_pairs += 1;
                if prev_cmp != curr_cmp {
                    flipped_pairs += 1;
                }
            }
        }

        if comparable_pairs == 0 {
            0.0
        } else {
            (flipped_pairs as f64) / (comparable_pairs as f64)
        }
    }

    // // OPTION 4: COST-BASED TRIGGER
    // fn should_replan_cost_based(&self, previous_stats: &ContainerStats) -> bool {
    //     self.cost_improvement(previous_stats) > 0.1
    // }

    // fn cost_improvement(&self, previous_stats: &ContainerStats) -> f64 {
        
    // }

    // LAST OPTION: Hybrid
    pub fn should_replan_hybrid(&self, previous_stats: &ContainerStats) -> bool {
        if self.should_replan_size_change(previous_stats) {
            true
        } else if self.should_replan_predicate_distribution(previous_stats) {
            true
        } else if self.should_replan_rank_change(previous_stats) {
            true
        } else {
            false
        }
    }

}

fn compare_with_epsilon(a: f64, b: f64, epsilon: f64) -> i8 {
    if (a - b).abs() <= epsilon {
        0
    } else if a < b {
        -1
    } else {
        1
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

    fn make_stats_from_predicates(predicate_counts: &[(u32, i64)]) -> ContainerStats {
        let mut predicate_cardinalities = HashMap::new();
        let total_triples = predicate_counts.iter().map(|(_, c)| *c).sum();

        for (predicate, count) in predicate_counts.iter().copied() {
            predicate_cardinalities.insert(predicate, count);
        }

        ContainerStats {
            total_triples,
            predicate_cardinalities,
            subject_cardinalities: HashMap::new(),
            object_cardinalities: HashMap::new(),
        }
    }

    fn make_stats_from_subjects(subject_counts: &[(u32, i64)]) -> ContainerStats {
        let mut subject_cardinalities = HashMap::new();
        let total_triples = subject_counts.iter().map(|(_, c)| *c).sum();

        for (subject, count) in subject_counts.iter().copied() {
            subject_cardinalities.insert(subject, count);
        }

        ContainerStats {
            total_triples,
            predicate_cardinalities: HashMap::new(),
            subject_cardinalities,
            object_cardinalities: HashMap::new(),
        }
    }

    fn make_stats_from_objects(object_counts: &[(u32, i64)]) -> ContainerStats {
        let mut object_cardinalities = HashMap::new();
        let total_triples = object_counts.iter().map(|(_, c)| *c).sum();

        for (object, count) in object_counts.iter().copied() {
            object_cardinalities.insert(object, count);
        }

        ContainerStats {
            total_triples,
            predicate_cardinalities: HashMap::new(),
            subject_cardinalities: HashMap::new(),
            object_cardinalities,
        }
    }
    
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

    #[test]
    fn test_should_replan_count_distribution_true_on_large_shift() {
        let previous_stats = make_stats_from_predicates(&[(1, 8), (2, 2)]);
        let current_stats = make_stats_from_predicates(&[(1, 2), (2, 8)]);

        assert!(current_stats.should_replan_predicate_distribution(&previous_stats));
    }

    #[test]
    fn test_should_replan_count_distribution_false_on_small_shift() {
        let previous_stats = make_stats_from_predicates(&[(1, 6), (2, 4)]);
        let current_stats = make_stats_from_predicates(&[(1, 5), (2, 5)]);

        assert!(!current_stats.should_replan_predicate_distribution(&previous_stats));
    }

    #[test]
    fn test_should_replan_basic_true_on_rank_flip() {
        let previous_stats = make_stats_from_predicates(&[(1, 1), (2, 2), (3, 4)]);
        let current_stats = make_stats_from_predicates(&[(1, 3), (2, 2), (3, 4)]);

        // One of three predicate pairs flips order => 0.333 > 0.2
        assert!(current_stats.should_replan_rank_change(&previous_stats));
    }

    #[test]
    fn test_should_replan_basic_false_when_rank_stable() {
        let previous_stats = make_stats_from_predicates(&[(1, 1), (2, 2), (3, 4)]);
        let current_stats = make_stats_from_predicates(&[(1, 2), (2, 3), (3, 6)]);

        assert!(!current_stats.should_replan_rank_change(&previous_stats));
    }

    #[test]
    fn test_should_replan_subject_distribution_true_on_large_shift() {
        let previous_stats = make_stats_from_subjects(&[(10, 8), (11, 2)]);
        let current_stats = make_stats_from_subjects(&[(10, 2), (11, 8)]);

        assert!(current_stats.should_replan_subject_distribution(&previous_stats));
    }

    #[test]
    fn test_should_replan_object_distribution_false_on_small_shift() {
        let previous_stats = make_stats_from_objects(&[(100, 6), (101, 4)]);
        let current_stats = make_stats_from_objects(&[(100, 5), (101, 5)]);

        assert!(!current_stats.should_replan_object_distribution(&previous_stats));
    }

    #[test]
    fn test_should_replan_subject_rank_change_true_on_flip() {
        let previous_stats = make_stats_from_subjects(&[(1, 1), (2, 2), (3, 4)]);
        let current_stats = make_stats_from_subjects(&[(1, 3), (2, 2), (3, 4)]);

        assert!(current_stats.should_replan_subject_rank_change(&previous_stats));
    }

    #[test]
    fn test_should_replan_object_rank_change_false_when_rank_stable() {
        let previous_stats = make_stats_from_objects(&[(1, 1), (2, 2), (3, 4)]);
        let current_stats = make_stats_from_objects(&[(1, 2), (2, 3), (3, 6)]);

        assert!(!current_stats.should_replan_object_rank_change(&previous_stats));
    }
}
