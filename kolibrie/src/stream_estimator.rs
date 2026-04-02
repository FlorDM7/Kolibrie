use shared::{query::FilterExpression, terms::{Term, TriplePattern}};
use crate::{container_stats::ContainerStats, streamertail_optimizer::{Condition, CostConstants, PhysicalOperator}};

/**
 * Estimate the cost of a logical plan given ContainerStats from a window.
 * These stats include total amount of triples and some cardinalities.
 * This is inspired by the CostEstimator struct used in the estimator.rs
 */

pub struct StreamEstimator {
    stats: ContainerStats
}

impl StreamEstimator {
    // Create a new stream estimator with the given statistics
    pub fn new(stats: ContainerStats) -> Self {
        Self {stats}
    }

    // Estimate cost of a logical plan
    // Based on estimator.rs
    pub fn estimate_cost(&self, plan: &PhysicalOperator) -> Option<i64> {
        match plan {
            // Scan
            PhysicalOperator::TableScan { pattern } => {
                Some(self.estimate_cardinality(pattern) * CostConstants::COST_PER_ROW_SCAN as i64)
            }
            // Selection
            PhysicalOperator::Filter { input, condition } => {
                let input_cost = self.estimate_cost(input).unwrap();
                let selectivity = self.estimate_selectivity(condition);
                Some((input_cost as f64 * selectivity) as i64 + CostConstants::COST_PER_FILTER as i64)
            }
            // Projection
            PhysicalOperator::Projection { input, .. } => {
                Some(self.estimate_cost(input).unwrap() + CostConstants::COST_PER_PROJECTION as i64)
            }
            // Join
            PhysicalOperator::HashJoin { left, right } => {
                let left_cost = self.estimate_cost(left).unwrap();
                let right_cost = self.estimate_cost(right).unwrap();
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                Some(
                left_cost
                    + right_cost
                    + (left_cardinality * right_cardinality)
                        * CostConstants::COST_PER_ROW_NESTED_LOOP as i64
                )
            }
            // Other operators not implemented
            _ => None 
        }
    }

    // Estimates the cardinality of a triple pattern
    // from estimator.rs
    pub fn estimate_cardinality(&self, pattern: &TriplePattern) -> i64 {
        match pattern {
            // Fully bound - always returns 0 or 1
            (Term::Constant(_), Term::Constant(_), Term::Constant(_)) => 1,

            // Two bounds - use actual index stats
            (Term::Constant(s), Term::Constant(p), Term::Variable(_)) => {
                // Look up actual SPO cardinality
                self.stats.get_subject_cardinality(*s)
                    .min(self.stats.get_predicate_cardinality(*p))
                    .max(1)
            }

            (Term::Constant(s), Term::Variable(_), Term::Constant(o)) => {
                // S*O pattern
                self.stats.get_subject_cardinality(*s)
                    .min(self.stats.get_object_cardinality(*o))
                    .max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Constant(o)) => {
                // *PO pattern
                self.stats.get_predicate_cardinality(*p)
                    .min(self.stats.get_object_cardinality(*o))
                    .max(1)
            }

            // One bound - use predicate/subject/object cardinality directly
            (Term::Constant(s), Term::Variable(_), Term::Variable(_)) => {
                self. stats.get_subject_cardinality(*s).max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Variable(_)) => {
                // This is the KEY one - should return ACTUAL predicate cardinality!
                self.stats.get_predicate_cardinality(*p).max(1)
            }

            (Term::Variable(_), Term::Variable(_), Term::Constant(o)) => {
                self.stats.get_object_cardinality(*o).max(1)
            }

            // No bounds - full scan
            (Term::Variable(_), Term::Variable(_), Term::Variable(_)) => {
                self.stats.total_triples
            }

            // Quoted triple not implemented
            _ => 1
        }
    }

    /// Estimates the selectivity of a condition
    /// from estimator.rs
    pub fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        self.estimate_filter_selectivity(&condition.expression)
    }

    /// Recursively estimates the selectivity of a filter expression
    /// from estimator.rs
    fn estimate_filter_selectivity(&self, expr: &FilterExpression) -> f64 {
        match expr {
            FilterExpression::Comparison(_, op, _) => {
                match *op {
                    "=" => 0.05,  // Equality is very selective
                    "!=" => 0.95, // Not equal is not very selective
                    ">" | "<" => 0.25,  // Range queries
                    ">=" | "<=" => 0.30,
                    _ => 0.5,  // Unknown operators
                }
            }
            FilterExpression::And(left, right) => {
                // AND is more selective - multiply selectivities
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel * right_sel
            }
            FilterExpression::Or(left, right) => {
                // OR is less selective - use formula: sel(A OR B) = sel(A) + sel(B) - sel(A)*sel(B)
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel + right_sel - (left_sel * right_sel)
            }
            FilterExpression::Not(inner) => {
                // NOT inverts selectivity
                let inner_sel = self.estimate_filter_selectivity(inner);
                1.0 - inner_sel
            }
            FilterExpression::ArithmeticExpr(_) => {
                // Conservative estimate for arithmetic expressions
                0.5
            }

            // Other not implemented
            _ => 1.0
        }
    }

    /// Extracts the predicate ID from a logical operator if it's a scan
    /// from estimator.rs but now logical
    fn extract_predicate_from_logical(&self, plan: &PhysicalOperator) -> Option<u32> {
        match plan {
            PhysicalOperator::TableScan { pattern } => {
                if let Term::Constant(pred_id) = pattern.1 {
                    Some(pred_id)
                } else {
                    None
                }
            }
            PhysicalOperator::Filter { input, ..  } => self.extract_predicate_from_logical(input),
            PhysicalOperator::Projection { input, .. } => self.extract_predicate_from_logical(input),
            _ => None,
        }
    }

    /// Computes join selectivity based on actual statistics
    /// based on same method in estimator.rs
    fn compute_join_selectivity(&self, left: &PhysicalOperator, right: &PhysicalOperator) -> f64 {
        let left_predicate = self.extract_predicate_from_logical(left);
        let right_predicate = self.extract_predicate_from_logical(right);

        match (left_predicate, right_predicate) {
            (Some(pred), _) => self.stats.get_join_selectivity(pred),
            (None, Some(pred)) => self. stats.get_join_selectivity(pred),
            (None, None) => 0.1, // Fallback
        }
    }

    /// Estimates the output cardinality of a physical operator
    /// based on same method in estimator.rs
    pub fn estimate_output_cardinality(&self, plan: &PhysicalOperator) -> i64 {
        match plan {
            PhysicalOperator::TableScan { pattern } => self.estimate_cardinality(pattern),
            PhysicalOperator::Filter { input, condition } => {
                let input_cardinality = self.estimate_output_cardinality(input);
                let selectivity = self.estimate_selectivity(condition);
                ((input_cardinality as f64 * selectivity) as i64).max(1)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let join_selectivity = self.compute_join_selectivity(left, right);
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as i64).max(1)
            }
            PhysicalOperator::Projection { input, .. } => self.estimate_output_cardinality(input),
            _ => 0
        }
    }
}