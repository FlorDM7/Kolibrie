use crate::streamertail_optimizer::*;
use crate::sparql_database::SparqlDatabase;
use std::collections::HashSet;

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
    println!("{} query plans considered", plans.len());
    for plan in plans {
        let physical_plan = logical_to_physical(plan); // Turn into fysical operators 
        let cost = estimator.estimate_cost(&physical_plan); // Estimate cost
        // dbg!(cost);
        // dbg!(&physical_plan);
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
            PhysicalOperator::Subquery { inner: Box::new(logical_to_physical(*inner)), projected_vars },
        _ => {
            panic!("I don't know about the other operators okay");
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