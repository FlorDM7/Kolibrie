use crate::container_stats::ContainerStats;
use crate::stream_estimator::StreamEstimator;
use crate::streamertail_optimizer::*;
use crate::sparql_database::SparqlDatabase;
use std::collections::HashSet;

/**
 * Reordering based on a ContentContainer with a StreamEstimator
 */
pub fn recalculate_window_plan(logical_plan: PhysicalOperator, container_stats: ContainerStats) -> PhysicalOperator {
    let plans = generate_all_neighbouring_plans(&logical_plan, &container_stats);
    let mut result = plans.get(0).unwrap().clone(); // initialize result variable
    let estimator = StreamEstimator::new(container_stats);
    let mut minimum_estimated_cost = i64::MAX;
    // println!("[Recalculate] {} query plans considered", plans.len());
    for plan in plans {
        let cost = estimator.estimate_cost(&plan).unwrap(); // Estimate cost
        // println!("{}", cost);
        if cost < minimum_estimated_cost {
            result = plan;
            minimum_estimated_cost = cost; // Minimalize cost
        }
    }
    // dbg!(minimum_estimated_cost);
    result.clone()
}

pub fn calculate_initial_window_plan(logical_plan: LogicalOperator, container_stats: ContainerStats) -> PhysicalOperator {
    let plans = generate_all_reorderings(&logical_plan);
    let mut result = logical_to_physical(plans.get(0).unwrap().clone()); // initialize result variable
    let estimator = StreamEstimator::new(container_stats);
    let mut minimum_estimated_cost = i64::MAX;
    // println!("[Initial] {} query plans considered", plans.len());
    for plan in plans {
        let plan = logical_to_physical(plan);
        let cost = estimator.estimate_cost(&plan).unwrap(); // Estimate cost
        // println!("{}", cost);
        if cost < minimum_estimated_cost {
            result = plan;
            minimum_estimated_cost = cost; // Minimalize cost
        }
    }
    result.clone()
}

/*
 Naive static join reordering
 Generate all possible join reoderings and pick the "best" one
 */

pub fn naive_reordering(logical_plan: LogicalOperator, db: &mut SparqlDatabase) -> PhysicalOperator {
    let plans = generate_all_reorderings(&logical_plan);
    pick_best_one(plans, db)
}

pub fn pick_best_one(plans: Vec<LogicalOperator>, db: &mut SparqlDatabase) -> PhysicalOperator {
    let mut result = logical_to_physical(plans.get(0).unwrap().clone());
    let binding = &DatabaseStats::gather_stats_fast(db);
    let estimator = CostEstimator::new(binding); // Use cost estimator from volcano (make stream estimator)
    let mut minimum_estimated_cost = u64::MAX;
    // println!("{} query plans considered", plans.len());
    for plan in plans {
        let physical_plan = logical_to_physical(plan); // Turn into fysical operators 
        let cost = estimator.estimate_cost(&physical_plan); // Estimate cost
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
            PhysicalOperator::HashJoin { left: Box::new(logical_to_physical(*left)), right: Box::new(logical_to_physical(*right))},
        LogicalOperator::Subquery { inner, projected_vars } =>
            PhysicalOperator::Subquery { inner: Box::new(logical_to_physical(*inner)), projected_vars },
        _ => {
            panic!("I don't know about the other operators!");
        }
    }
}

// Return a list with all possible join reorderings
pub fn generate_all_reorderings(logical_plan: &LogicalOperator) -> Vec<LogicalOperator> {
    let plan = pushdown_filters(logical_plan.clone());
    let (core_plan, top_ops) = strip_top_level_ops(plan);
    // collect all scans
    let mut all_scans: Vec<LogicalOperator> = find_all_scans(core_plan);
    // perform joins in all possible different ways
    let plans: Vec<LogicalOperator> = generate_all_plans(&mut all_scans);
    plans
        .into_iter()
        .map(|plan| apply_top_level_ops(plan, &top_ops))
        .collect()
}

/*
GENERATE NEIGHBOURING PLANS
With GenAI
 */
// Return a small neighborhood of plans by swapping one or two joins in the existing tree.
pub fn generate_all_neighbouring_plans(logical_plan: &PhysicalOperator, container_stats: &ContainerStats) -> Vec<PhysicalOperator> {
    let (core_plan, top_ops) = strip_top_level_physical_ops(logical_plan.clone());

    let mut candidates: Vec<PhysicalOperator> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut add_candidate = |candidate: PhysicalOperator| {
        let key = format!("{:?}", candidate);
        if seen.insert(key) {
            candidates.push(candidate);
        }
    };

    // Add the original plan
    add_candidate(core_plan.clone());

    // Generate tree-restructured alternatives with stats-guided scan ordering.
    let restructured = restructure_plan_tree(&core_plan, container_stats);
    for plan in restructured {
        add_candidate(plan);
    }

    // // Generate 1-swap neighbors
    // for path in &join_paths {
    //     if let Some(swapped) = swap_join_children_at_path_physical(&core_plan, path) {
    //         add_candidate(swapped);
    //     }
    // }

    // // Generate 2-swap neighbors
    // for i in 0..join_paths.len() {
    //     for j in (i + 1)..join_paths.len() {
    //         let mut candidate = core_plan.clone();
    //         if let Some(swapped_once) = swap_join_children_at_path_physical(&candidate, &join_paths[i]) {
    //             candidate = swapped_once;
    //         } else {
    //             continue;
    //         }

    //         if let Some(swapped_twice) = swap_join_children_at_path_physical(&candidate, &join_paths[j]) {
    //             add_candidate(swapped_twice);
    //         }
    //     }
    // }

    // List of unique neighboring plans
    candidates
        .into_iter()
        .map(|plan| apply_top_level_physical_ops(plan, &top_ops))
        .collect()
}

// Collect all paths for where there is a join that we can swap
fn collect_join_paths_physical(plan: &PhysicalOperator, current_path: &mut Vec<usize>, paths: &mut Vec<Vec<usize>>) {
    match plan {
        PhysicalOperator::HashJoin { left, right } => {
            paths.push(current_path.clone());

            current_path.push(0);
            collect_join_paths_physical(left, current_path, paths);
            current_path.pop();

            current_path.push(1);
            collect_join_paths_physical(right, current_path, paths);
            current_path.pop();
        }
        PhysicalOperator::Filter { input, .. } => {
            collect_join_paths_physical(input, current_path, paths);
        }
        PhysicalOperator::Projection { input, .. } => {
            collect_join_paths_physical(input, current_path, paths);
        }
        PhysicalOperator::Subquery { inner, .. } => {
            collect_join_paths_physical(inner, current_path, paths);
        }
        _ => {}
    }
}

// Swap the children of a HashJoin at an given path
// The path contains zeros to go left, one to go right in the tree
fn swap_join_children_at_path_physical(plan: &PhysicalOperator, path: &[usize]) -> Option<PhysicalOperator> {
    // Base case: we arrived at the join we wan to swap
    if path.is_empty() {
        return match plan {
            PhysicalOperator::HashJoin { left, right } => Some(PhysicalOperator::HashJoin {
                left: right.clone(),
                right: left.clone(),
            }),
            _ => None,
        };
    }

    // Recursive case
    match plan {
        PhysicalOperator::HashJoin { left, right } => {
            // We go into the left part of the join
            if path[0] == 0 {
                swap_join_children_at_path_physical(left, &path[1..]).map(|new_left| PhysicalOperator::HashJoin {
                    left: Box::new(new_left),
                    right: right.clone(),
                })
            // We go into the right part of the join
            } else if path[0] == 1 {
                swap_join_children_at_path_physical(right, &path[1..]).map(|new_right| PhysicalOperator::HashJoin {
                    left: left.clone(),
                    right: Box::new(new_right),
                })
            } else {
                None
            }
        }
        PhysicalOperator::Filter { input, condition } => {
            swap_join_children_at_path_physical(input, path).map(|new_input| PhysicalOperator::Filter {
                input: Box::new(new_input),
                condition: condition.clone(),
            })
        }
        PhysicalOperator::Projection { input, variables } => {
            swap_join_children_at_path_physical(input, path).map(|new_input| PhysicalOperator::Projection {
                input: Box::new(new_input),
                variables: variables.clone(),
            })
        }
        PhysicalOperator::Subquery { inner, projected_vars } => {
            swap_join_children_at_path_physical(inner, path).map(|new_inner| PhysicalOperator::Subquery {
                inner: Box::new(new_inner),
                projected_vars: projected_vars.clone(),
            })
        }
        _ => None,
    }
}

/**
 * Generate alternative tree structures by reordering and restructuring leaf scans
 * In this case: left deep tree, right deep tree, balanced tree, revered tree
 */

// Extract all leaf table scans from a physical operator tree
// Returns a vector of TableScan operators in order encountered
fn extract_leaf_scans_physical(plan: &PhysicalOperator) -> Vec<PhysicalOperator> {
    let mut scans = Vec::new();
    extract_leaf_scans_physical_helper(plan, &mut scans);
    scans
}

fn extract_leaf_scans_physical_helper(plan: &PhysicalOperator, scans: &mut Vec<PhysicalOperator>) {
    match plan {
        PhysicalOperator::HashJoin { left, right } => {
            extract_leaf_scans_physical_helper(left, scans);
            extract_leaf_scans_physical_helper(right, scans);
        }
        PhysicalOperator::Filter { input, .. } => {
            extract_leaf_scans_physical_helper(input, scans);
        }
        PhysicalOperator::Projection { input, .. } => {
            extract_leaf_scans_physical_helper(input, scans);
        }
        PhysicalOperator::TableScan { pattern } => {
            scans.push(PhysicalOperator::TableScan { pattern: pattern.clone() });
        }
        _ => {
            // For other leaf types, just include them as-is
            scans.push(plan.clone());
        }
    }
}

// Build a left-deep join tree: ((A join B) join C) join D
// Each scan joins with the result of previous joins on the left
fn build_left_deep_tree(scans: Vec<PhysicalOperator>) -> Option<PhysicalOperator> {
    if scans.is_empty() {
        return None;
    }
    if scans.len() == 1 {
        return Some(scans[0].clone());
    }

    let mut result = scans[0].clone();
    for scan in &scans[1..] {
        result = PhysicalOperator::HashJoin {
            left: Box::new(result),
            right: Box::new(scan.clone()),
        };
    }
    Some(result)
}

// Build a right-deep join tree: A join (B join (C join D))
// Each scan joins with the result of remaining scans on the right
fn build_right_deep_tree(scans: Vec<PhysicalOperator>) -> Option<PhysicalOperator> {
    if scans.is_empty() {
        return None;
    }
    if scans.len() == 1 {
        return Some(scans[0].clone());
    }

    let mut result = scans[scans.len() - 1].clone();
    for i in (0..scans.len() - 1).rev() {
        result = PhysicalOperator::HashJoin {
            left: Box::new(scans[i].clone()),
            right: Box::new(result),
        };
    }
    Some(result)
}

// Build a balanced join tree (zigzag pattern)
// Pairs up scans progressively to create a more balanced structure
fn build_balanced_tree(scans: Vec<PhysicalOperator>) -> Option<PhysicalOperator> {
    if scans.is_empty() {
        return None;
    }
    if scans.len() == 1 {
        return Some(scans[0].clone());
    }

    let mut current_level = scans.clone();
    
    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        
        // Pair up adjacent scans at this level
        for i in (0..current_level.len()).step_by(2) {
            if i + 1 < current_level.len() {
                next_level.push(PhysicalOperator::HashJoin {
                    left: Box::new(current_level[i].clone()),
                    right: Box::new(current_level[i + 1].clone()),
                });
            } else {
                // Odd one out, carry to next level
                next_level.push(current_level[i].clone());
            }
        }
        
        current_level = next_level;
    }
    
    current_level.pop()
}

// Generate alternative tree structures by reordering and restructuring leaf scans
// This produces meaningfully different join orders (not just child swaps)
// Returns several alternative tree structures with the same leaves
fn restructure_plan_tree(plan: &PhysicalOperator, container_stats: &ContainerStats) -> Vec<PhysicalOperator> {
    // Extract all leaf scans from the current plan
    let mut leaf_scans = extract_leaf_scans_physical(plan);
    
    // Need at least 2 scans to make restructuring worthwhile
    if leaf_scans.len() < 2 {
        return vec![];
    }

    // Prefer starting from the most selective scans based on observed window stats.
    leaf_scans.sort_by_key(|scan| {
        estimate_scan_cardinality_for_sort(scan, container_stats)
    });

    let mut alternatives = Vec::new();

    // Generate different tree structures with same scans
    if let Some(left_deep) = build_left_deep_tree(leaf_scans.clone()) {
        alternatives.push(left_deep);
    }

    if let Some(right_deep) = build_right_deep_tree(leaf_scans.clone()) {
        alternatives.push(right_deep);
    }

    if let Some(balanced) = build_balanced_tree(leaf_scans.clone()) {
        alternatives.push(balanced);
    }

    // Generate pair-first alternatives to vary which leaves are joined earliest.
    if leaf_scans.len() <= 6 {
        for i in 0..leaf_scans.len() {
            for j in (i + 1)..leaf_scans.len() {
                if let Some(pair_first) = build_pair_first_tree(&leaf_scans, i, j) {
                    alternatives.push(pair_first);
                }
            }
        }
    }

    // // Generate a variant with reversed scan order (first scan pairs with last)
    // let mut reversed_scans = leaf_scans.clone();
    // reversed_scans.reverse();
    // if let Some(reversed_left_deep) = build_left_deep_tree(reversed_scans) {
    //     alternatives.push(reversed_left_deep);
    // }

    alternatives
}

fn estimate_scan_cardinality_for_sort(scan: &PhysicalOperator, container_stats: &ContainerStats) -> i64 {
    match scan {
        PhysicalOperator::TableScan { pattern } => {
            let total = container_stats.get_total_triples().max(1);
            let avg_subject = (total / (container_stats.get_total_subjects().max(1) as i64)).max(1);
            let avg_predicate = (total / (container_stats.get_total_predicates().max(1) as i64)).max(1);
            let avg_object = (total / (container_stats.get_total_objects().max(1) as i64)).max(1);

            let subject_estimate = match &pattern.0 {
                shared::terms::Term::Constant(subject) => container_stats.get_subject_cardinality(*subject).max(1),
                _ => avg_subject,
            };  
            let predicate_estimate = match &pattern.1 {
                shared::terms::Term::Constant(predicate) => container_stats.get_predicate_cardinality(*predicate).max(1),
                _ => avg_predicate,
            };
            let object_estimate = match &pattern.2 {
                shared::terms::Term::Constant(object) => container_stats.get_object_cardinality(*object).max(1),
                _ => avg_object,
            };

            let mut bound_terms = 0;
            if matches!(pattern.0, shared::terms::Term::Constant(_)) {
                bound_terms += 1;
            }
            if matches!(pattern.1, shared::terms::Term::Constant(_)) {
                bound_terms += 1;
            }
            if matches!(pattern.2, shared::terms::Term::Constant(_)) {
                bound_terms += 1;
            }

            // Favors more bound triple patterns first, then lower estimated cardinality.
            // Keep the key non-negative: lower key means better scan to join earlier.
            let selectivity_score = (subject_estimate + predicate_estimate + object_estimate).max(1);
            let unbound_penalty = ((3 - bound_terms) as i64) * total;
            selectivity_score + unbound_penalty
        }
        _ => container_stats.get_total_triples().max(1),
    }
}

fn build_pair_first_tree(scans: &[PhysicalOperator], first_idx: usize, second_idx: usize) -> Option<PhysicalOperator> {
    if scans.len() < 2 || first_idx >= scans.len() || second_idx >= scans.len() || first_idx == second_idx {
        return None;
    }

    let mut result = PhysicalOperator::HashJoin {
        left: Box::new(scans[first_idx].clone()),
        right: Box::new(scans[second_idx].clone()),
    };

    for (idx, scan) in scans.iter().enumerate() {
        if idx == first_idx || idx == second_idx {
            continue;
        }
        result = PhysicalOperator::HashJoin {
            left: Box::new(result),
            right: Box::new(scan.clone()),
        };
    }

    Some(result)
}

/*
GENERATE ALL PLANS
By me, but filters with genAI 
 */

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
            if is_scan_subtree(&predicate) {
                result.push(LogicalOperator::Selection { predicate, condition });
                result
            } else {
                let mut list = find_all_scans(*predicate);
                result.append(&mut list);
                result
            }
        },
        LogicalOperator::Projection { predicate, variables: _variables } => {
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

// vibe
#[derive(Clone, Debug)]
enum TopLevelOp {
    Selection(Condition),
    Projection(Vec<String>),
}

#[derive(Clone, Debug)]
enum TopLevelPhysicalOp {
    Filter(Condition),
    Projection(Vec<String>),
}

// vibe
fn strip_top_level_ops(logical_plan: LogicalOperator) -> (LogicalOperator, Vec<TopLevelOp>) {
    let mut ops = Vec::new();
    let mut current = logical_plan;

    loop {
        match current {
            LogicalOperator::Selection { predicate, condition } => {
                ops.push(TopLevelOp::Selection(condition));
                current = *predicate;
            }
            LogicalOperator::Projection { predicate, variables } => {
                ops.push(TopLevelOp::Projection(variables));
                current = *predicate;
            }
            _ => break,
        }
    }

    (current, ops)
}

// vibe
fn apply_top_level_ops(plan: LogicalOperator, ops: &[TopLevelOp]) -> LogicalOperator {
    let mut current = plan;
    for op in ops.iter().rev() {
        current = match op {
            TopLevelOp::Selection(condition) => LogicalOperator::selection(current, condition.clone()),
            TopLevelOp::Projection(variables) => {
                LogicalOperator::projection(current, variables.clone())
            }
        };
    }

    current
}

fn strip_top_level_physical_ops(physical_plan: PhysicalOperator) -> (PhysicalOperator, Vec<TopLevelPhysicalOp>) {
    let mut ops = Vec::new();
    let mut current = physical_plan;

    loop {
        match current {
            PhysicalOperator::Filter { input, condition } => {
                ops.push(TopLevelPhysicalOp::Filter(condition));
                current = *input;
            }
            PhysicalOperator::Projection { input, variables } => {
                ops.push(TopLevelPhysicalOp::Projection(variables));
                current = *input;
            }
            _ => break,
        }
    }

    (current, ops)
}

fn apply_top_level_physical_ops(plan: PhysicalOperator, ops: &[TopLevelPhysicalOp]) -> PhysicalOperator {
    let mut current = plan;
    for op in ops.iter().rev() {
        current = match op {
            TopLevelPhysicalOp::Filter(condition) => PhysicalOperator::Filter {
                input: Box::new(current),
                condition: condition.clone(),
            },
            TopLevelPhysicalOp::Projection(variables) => PhysicalOperator::Projection {
                input: Box::new(current),
                variables: variables.clone(),
            },
        };
    }

    current
}

// vibe
fn pushdown_filters(logical_plan: LogicalOperator) -> LogicalOperator {
    match logical_plan {
        LogicalOperator::Selection { predicate, condition } => {
            let predicate = pushdown_filters(*predicate);
            match predicate {
                LogicalOperator::Join { left, right } => {
                    let filter_vars = collect_filter_vars(&condition);
                    let left_vars = collect_plan_vars(&left);
                    let right_vars = collect_plan_vars(&right);

                    if !filter_vars.is_empty() && filter_vars.is_subset(&left_vars) {
                        let new_left = LogicalOperator::selection(*left, condition);
                        LogicalOperator::Join {
                            left: Box::new(pushdown_filters(new_left)),
                            right,
                        }
                    } else if !filter_vars.is_empty() && filter_vars.is_subset(&right_vars) {
                        let new_right = LogicalOperator::selection(*right, condition);
                        LogicalOperator::Join {
                            left,
                            right: Box::new(pushdown_filters(new_right)),
                        }
                    } else {
                        LogicalOperator::Selection {
                            predicate: Box::new(LogicalOperator::Join { left, right }),
                            condition,
                        }
                    }
                }
                other => LogicalOperator::Selection {
                    predicate: Box::new(other),
                    condition,
                },
            }
        }
        LogicalOperator::Join { left, right } => LogicalOperator::Join {
            left: Box::new(pushdown_filters(*left)),
            right: Box::new(pushdown_filters(*right)),
        },
        LogicalOperator::Projection { predicate, variables } => LogicalOperator::Projection {
            predicate: Box::new(pushdown_filters(*predicate)),
            variables,
        },
        LogicalOperator::Bind {
            input,
            function_name,
            arguments,
            output_variable,
        } => LogicalOperator::Bind {
            input: Box::new(pushdown_filters(*input)),
            function_name,
            arguments,
            output_variable,
        },
        LogicalOperator::Subquery { inner, projected_vars } => LogicalOperator::Subquery {
            inner: Box::new(pushdown_filters(*inner)),
            projected_vars,
        },
        LogicalOperator::MLPredict {
            input,
            model_name,
            input_variables,
            output_variable,
        } => LogicalOperator::MLPredict {
            input: Box::new(pushdown_filters(*input)),
            model_name,
            input_variables,
            output_variable,
        },
        LogicalOperator::Values { .. }
        | LogicalOperator::Scan { .. }
        | LogicalOperator::Buffer { .. } => logical_plan,
    }
}

// vibe
fn collect_filter_vars(condition: &Condition) -> HashSet<String> {
    let mut vars = HashSet::new();
    collect_filter_vars_from_expr(&condition.expression, &mut vars);
    vars
}

// vibe
fn collect_filter_vars_from_expr(expr: &shared::query::FilterExpression, vars: &mut HashSet<String>) {
    match expr {
        shared::query::FilterExpression::Comparison(var, _, _) => {
            let normalized = var.strip_prefix('?').unwrap_or(var);
            vars.insert(normalized.to_string());
        }
        shared::query::FilterExpression::And(left, right)
        | shared::query::FilterExpression::Or(left, right) => {
            collect_filter_vars_from_expr(left, vars);
            collect_filter_vars_from_expr(right, vars);
        }
        shared::query::FilterExpression::Not(inner) => {
            collect_filter_vars_from_expr(inner, vars);
        }
        shared::query::FilterExpression::ArithmeticExpr(_) => {}
        
        // Ohters not implemented
        _ => ()
    }
}

// vibe
fn collect_plan_vars(plan: &LogicalOperator) -> HashSet<String> {
    match plan {
        LogicalOperator::Scan { pattern } => collect_pattern_vars(pattern),
        LogicalOperator::Selection { predicate, .. } => collect_plan_vars(predicate),
        LogicalOperator::Projection { variables, .. } => variables.iter().cloned().collect(),
        LogicalOperator::Join { left, right } => {
            let mut vars = collect_plan_vars(left);
            vars.extend(collect_plan_vars(right));
            vars
        }
        LogicalOperator::Subquery { projected_vars, .. } => projected_vars.iter().cloned().collect(),
        LogicalOperator::Bind { input, output_variable, .. } => {
            let mut vars = collect_plan_vars(input);
            vars.insert(output_variable.clone());
            vars
        }
        LogicalOperator::Values { variables, .. } => variables.iter().cloned().collect(),
        LogicalOperator::Buffer { .. } => HashSet::new(),
        LogicalOperator::MLPredict { input, output_variable, .. } => {
            let mut vars = collect_plan_vars(input);
            vars.insert(output_variable.clone());
            vars
        }
    }
}

// vibe
fn collect_pattern_vars(pattern: &shared::terms::TriplePattern) -> HashSet<String> {
    let mut vars = HashSet::new();
    for term in [&pattern.0, &pattern.1, &pattern.2] {
        if let shared::terms::Term::Variable(name) = term {
            vars.insert(name.clone());
        }
    }
    vars
}

// vibe
fn is_scan_subtree(plan: &LogicalOperator) -> bool {
    match plan {
        LogicalOperator::Scan { .. } => true,
        LogicalOperator::Selection { predicate, .. } => is_scan_subtree(predicate),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::terms::Term;

    #[test]
    fn test_generate_all_reorderings() {
        // Similar to example in simple_volcano.rs

        let database = SparqlDatabase::new();
        let mut dict = database.dictionary.write().unwrap(); // Get lock
        let name_id = dict.encode("http://example.org/name");
        let age_id = dict.encode("http://example.org/age");
        let works_at_id = dict.encode("http://example.org/worksAt");
        drop(dict); // Release lock early

        // Create a logical plan: join names with ages with company 
        let name_scan = LogicalOperator::scan((
            Term::Variable("person".to_string()),
            Term::Constant(name_id),
            Term::Variable("name".to_string()),
        ));

        let age_scan = LogicalOperator::scan((
            Term::Variable("person".to_string()),
            Term::Constant(age_id),
            Term::Variable("age".to_string()),
        ));

        let company_scan = LogicalOperator::scan((
            Term::Variable("person".to_string()),
            Term::Constant(works_at_id),
            Term::Variable("company".to_string()),
        ));

        let plan = LogicalOperator::join(name_scan, age_scan);
        let plan = LogicalOperator::join(plan, company_scan);

        /*
        Logical plan:
                     join
                    /    \
                 join   scan(company)
                /    \
        scan(name)  scan(age) 
        */

        let plans = generate_all_reorderings(&plan);
        assert_eq!(plans.len(), 3); // number of possible combinations
    }
}