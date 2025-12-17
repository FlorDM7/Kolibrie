/*
 Naive join reordering
 Generate all possible join reoderings and pick the "best" one
 */

use crate::volcano_optimizer::*;
use crate::sparql_database::SparqlDatabase;

pub fn pick_best_one(plans: Vec<LogicalOperator>, db: &mut SparqlDatabase) -> PhysicalOperator {
    let mut result = logical_to_physical(plans.get(0).unwrap().clone());
    let binding = &DatabaseStats::gather_stats_fast(db);
    let estimator = CostEstimator::new(binding); // stream estimator van maken, incrementeel aanpassen 
    let mut minimum_estimated_cost = u64::MAX;
    for plan in plans {
        let physical_plan = logical_to_physical(plan); // Turn into fysical operators 
        let cost = estimator.estimate_cost(&physical_plan); // Estimate cost
        dbg!(cost);
        dbg!(&physical_plan);
        if cost < minimum_estimated_cost {
            result = physical_plan;
            minimum_estimated_cost = cost; // Minimalize cost
        }
    }
    result
}

fn logical_to_physical(logical_plan: LogicalOperator) -> PhysicalOperator {
    match logical_plan {
        LogicalOperator::Scan { pattern } => 
            PhysicalOperator::TableScan { pattern },
        LogicalOperator::Projection { predicate, variables } => 
            PhysicalOperator::Projection { input: Box::new(logical_to_physical(*predicate)), variables },
        LogicalOperator::Selection { predicate, condition } =>
            PhysicalOperator::Filter { input: Box::new(logical_to_physical(*predicate)), condition },
        LogicalOperator::Join { left, right } =>
            PhysicalOperator::NestedLoopJoin { left: Box::new(logical_to_physical(*left)), right: Box::new(logical_to_physical(*right))},
        LogicalOperator::Subquery { inner, projected_vars } =>
            PhysicalOperator::Subquery { inner: Box::new(logical_to_physical(*inner)), projected_vars }
    }
}

// We assume we have a logical plan only consisting of scans and joins (for now)
// Return a list with all possible join reorderings
pub fn generate_all_reorderings(logical_plan: &LogicalOperator) -> Vec<LogicalOperator> {
    let plan = logical_plan.clone();
    // collect all scans
    let mut all_scans: Vec<LogicalOperator> = find_all_scans(plan);
    // perform joins in all possible different ways
    let result: Vec<LogicalOperator> = generate_all_plans(&mut all_scans);
    result
}

// Given a list of operators, recursively make a list of all possible logical plans where everything gets joined
// Initial call contains a list of Scan operators
fn generate_all_plans(operators: &mut Vec<LogicalOperator>) -> Vec<LogicalOperator> {
    if operators.len() < 1 {
        panic!("Input should not be empty");
    }
    let mut result: Vec<LogicalOperator> = Vec::new();
    // Base case: we only have one operator anymore, just return it
    if operators.len() == 1 {
        result.push(operators[0].clone());
        return result;
    }
    let length = operators.len(); // length >= 2
    // Loop over all combinations that for which we can make a join(i,j)
    for j in 0..length {
        let mut current_operators = operators.clone();
        let element = current_operators.remove(j);
        // Start from j, that way we don't have duplicate joins (i,j) (so (j,i) is not needed anymore)
        for i in j..length-1 { // for all i (> j, < length)
            let mut new_operators = current_operators.clone(); // Copy vector, because we will edit it, every time in the loop 
            let new_join = LogicalOperator::Join { left: Box::new(new_operators[i].clone()), right: Box::new(element.clone()) }; 
            // Update operator list
            new_operators.remove(i);
            new_operators.push(new_join);
            let mut subresult = generate_all_plans(&mut new_operators);
            result.append(&mut subresult);
        }
    }
    result
}

// Given a logical plan, this function looks recursively for all Scan operations 
// and returns a vector/list with all these scans
fn find_all_scans(logical_plan:LogicalOperator) -> Vec<LogicalOperator> {
    let mut result = Vec::new();
    match logical_plan {
        // Look in the arguments of the join for any scans
        LogicalOperator::Join { left, right } => {
            let mut left_list = find_all_scans(*left);
            result.append(&mut left_list);
            let mut right_list = find_all_scans(*right);
            result.append(&mut right_list);
            result
        },
        // Add scan to the list
        LogicalOperator::Scan { pattern } => {
            result.push(LogicalOperator::Scan { pattern });
            result
        },
        // Look in the predicate for a scan
        LogicalOperator::Selection { predicate, condition } => {
            let mut list = find_all_scans(*predicate);
            result.append(&mut list);
            result
        },
        LogicalOperator::Projection { predicate, variables } => {
            let mut list = find_all_scans(*predicate);
            result.append(&mut list);
            result
        }
        _ => {
            println!("Subquery not yet implemented!");
            result
        }
    }
}