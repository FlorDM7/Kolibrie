/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs;
use std::time::Instant;

use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::streamertail_optimizer::*;
use shared::terms::Term;
use shared::triple::Triple;

fn add_sample_data(database: &mut SparqlDatabase) {
    println!("Adding sample data...");

    // Add some triples about people

    // Subjects
    let alice_id = database.dictionary.encode("http://example.org/alice");
    let bob_id = database.dictionary.encode("http://example.org/bob");
    let charlie_id = database.dictionary.encode("http://example.org/charlie");

    // Predicates
    let name_id = database.dictionary.encode("http://example.org/name");
    let age_id = database.dictionary.encode("http://example.org/age");
    let works_at_id = database.dictionary.encode("http://example.org/worksAt");

    // Objects
    let alice_name = database.dictionary.encode("Alice");
    let bob_name = database.dictionary.encode("Bob");
    let charlie_name = database.dictionary.encode("Charlie");
    let age_25 = database.dictionary.encode("25");
    let age_30 = database.dictionary.encode("30");
    let age_35 = database.dictionary.encode("35");
    let age_36 = database.dictionary.encode("36");
    let age_37 = database.dictionary.encode("37");
    let age_38 = database.dictionary.encode("38");
    let age_39 = database.dictionary.encode("39");
    let age_40 = database.dictionary.encode("40");
    let age_41 = database.dictionary.encode("41");
    let age_42 = database.dictionary.encode("42");
    let company_id = database.dictionary.encode("http://example.org/company");
    let company2_id = database.dictionary.encode("http://example.org/company2");

    // Add triples
    // Alice is Alice
    database.add_triple(Triple {
        subject: alice_id,
        predicate: name_id,
        object: alice_name,
    });

    // Bob is bob
    database.add_triple(Triple {
        subject: bob_id,
        predicate: name_id,
        object: bob_name,
    });

    // Charlie is charlie
    database.add_triple(Triple {
        subject: charlie_id,
        predicate: name_id,
        object: charlie_name,
    });

    // Alice is 25
    database.add_triple(Triple {
        subject: alice_id,
        predicate: age_id,
        object: age_25,
    });

    // Bob is 30
    database.add_triple(Triple {
        subject: bob_id,
        predicate: age_id,
        object: age_30,
    });

    // But has different ages..., so we became different cost
    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_35,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_36,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_37,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_38,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_39,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_40,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_41,
    });

    // Alice works at company
    database.add_triple(Triple {
        subject: alice_id,
        predicate: works_at_id,
        object: company_id,
    });

    // Bob works at company2
    database.add_triple(Triple {
        subject: bob_id,
        predicate: works_at_id,
        object: company2_id,    // one person works in the other company
    });

    // Charlie works at company
    database.add_triple(Triple {
        subject: charlie_id,
        predicate: works_at_id,
        object: company_id,
    });

    println!(
        "Added {} triples to the database.\n",
        database.triples.len()
    );
}

fn multiple_join_query(database: &mut SparqlDatabase) {
    println!("=== Example: Multiple Join Query ===");

    let name_id = database.dictionary.encode("http://example.org/name");
    let age_id = database.dictionary.encode("http://example.org/age");
    let works_at_id = database.dictionary.encode("http://example.org/worksAt");

    // Create a logical plan: join names with ages with company 
    let name_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let name_scan2 = LogicalOperator::scan((
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

    let condition = Condition::new("company".to_string(), "=".to_string(), "http://example.org/company2".to_string());
    let logical_plan = LogicalOperator::join(name_scan, age_scan);
    let logical_plan = LogicalOperator::join(logical_plan, company_scan);
    // let logical_plan = LogicalOperator::join(logical_plan, name_scan2);
    // let logical_plan = LogicalOperator::selection(logical_plan, condition);

    /*
    Logical plan:
                    condition company='company2'   
                               | 
                             join
                            /    \
                         join    scan company
                        /    \
                scan name     scan age 
    */

    // Generate join reordering
    println!();
    println!("-=-=-=-=-=-");
    println!("All possible plans");
    let all_joins = kolibrie::join_reordering::generate_all_reorderings(&logical_plan);
    println!("-=-=-=-=-=-");
    println!();

    // Create optimizer and find best plan
    // let start = Instant::now();
    // let mut optimizer = VolcanoOptimizer::new(database);
    // let physical_plan = optimizer.find_best_plan(&logical_plan);
    // let optimization_time = start.elapsed();

    // println!("Optimization completed in {:?}", optimization_time);
    // println!("Physical plan: {:?}\n", physical_plan);

    let physical_plan = kolibrie::join_reordering::pick_best_one(all_joins, database);

    // Execute the plan
    let start = Instant::now();
    let results = physical_plan.execute(database);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results:", results.len());
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}

fn load_dataset(nt_path: &str) -> SparqlDatabase {
    let mut db = SparqlDatabase::new();
    let nt_data = fs::read_to_string(nt_path).expect("failed to read .nt file");
    db.parse_ntriples_and_add(&nt_data);
    db
}

fn example_multiple_join_query() {
    println!("=== Simple Volcano Optimizer Example ===\n");
    // Create a new database
    let mut database = SparqlDatabase::new();
    // Add some sample data
    add_sample_data(&mut database);
    multiple_join_query(&mut database);
    println!("=== Example completed successfully! ===");
}

// vibe
fn create_logical_plan(database: &mut SparqlDatabase) -> PhysicalOperator {
    let rdf_type_id = database
        .dictionary
        .encode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let temperature_sensor_id = database
        .dictionary
        .encode("http://example.org/TemperatureSensor");
    let located_in_id = database
        .dictionary
        .encode("http://example.org/locatedIn");
    let has_reading_id = database
        .dictionary
        .encode("http://example.org/hasReading");
    let value_id = database.dictionary.encode("http://example.org/value");
    let timestamp_id = database
        .dictionary
        .encode("http://example.org/timestamp");

    // Create the logical plan from the SPARQL query.
    let location_scan = LogicalOperator::scan((
        Term::Variable("sensor".to_string()),
        Term::Constant(located_in_id),
        Term::Variable("location".to_string()),
    ));

    let reading_scan = LogicalOperator::scan((
        Term::Variable("sensor".to_string()),
        Term::Constant(has_reading_id),
        Term::Variable("reading".to_string()),
    ));

    let value_scan = LogicalOperator::scan((
        Term::Variable("reading".to_string()),
        Term::Constant(value_id),
        Term::Variable("value".to_string()),
    ));

    let timestamp_scan = LogicalOperator::scan((
        Term::Variable("reading".to_string()),
        Term::Constant(timestamp_id),
        Term::Variable("timestamp".to_string()),
    ));

    let type_scan = LogicalOperator::scan((
        Term::Variable("sensor".to_string()),
        Term::Constant(rdf_type_id),
        Term::Constant(temperature_sensor_id),
    ));

    let joined = LogicalOperator::join(location_scan, reading_scan);
    let joined = LogicalOperator::join(joined, value_scan);
    let joined = LogicalOperator::join(joined, timestamp_scan);
    let joined = LogicalOperator::join(joined, type_scan);

    let condition = Condition::new("value".to_string(), ">".to_string(), "25.0".to_string());
    let filtered = LogicalOperator::selection(joined, condition);

    let logical_plan = LogicalOperator::projection(
        filtered,
        vec![
            "sensor".to_string(),
            "location".to_string(),
            "reading".to_string(),
            "timestamp".to_string(),
        ],
    );
    let physical_plan = kolibrie::join_reordering::naive_reordering(logical_plan, database);
    physical_plan
}

// Window as a seperate SparqlDatabase, this way we don't need to change anything in the
// reodering implementation and can estimate as before, but might not be so efficient
// vibe
fn build_window_db(source: &mut SparqlDatabase, window_size: u64) -> SparqlDatabase {
    let mut window_db = SparqlDatabase::new();
    window_db.dictionary = source.dictionary.clone();
    window_db.prefixes = source.prefixes.clone();

    // Find timestamp predicate ID
    let timestamp_id = source.dictionary.encode("http://example.org/timestamp");
    
    // Extract all unique timestamps from the dataset
    let mut timestamps: Vec<String> = Vec::new();
    for triple in &source.triples {
        if triple.predicate == timestamp_id {
            let timestamp_str = source.dictionary.decode(triple.object);
            timestamps.push(timestamp_str.unwrap().into());
        }
    }
    
    // Sort timestamps
    timestamps.sort();
    timestamps.dedup();
    
    // Take first N timestamps to define the window
    let window_limit = std::cmp::min(window_size as usize, timestamps.len());
    let window_timestamps: Vec<String> = timestamps.iter().take(window_limit).cloned().collect();
    
    if window_timestamps.is_empty() {
        window_db.triples = source.triples.clone();
        return window_db;
    }
    
    // Create a set of timestamp IDs in the window for fast lookup
    let window_timestamp_ids: std::collections::HashSet<u32> = window_timestamps
        .iter()
        .map(|ts| source.dictionary.encode(ts))
        .collect();
    
    // Filter triples: keep those with timestamps in the window
    for triple in &source.triples {
        if triple.predicate == timestamp_id {
            // This is a timestamp triple - include if it's in the window
            if window_timestamp_ids.contains(&triple.object) {
                window_db.add_triple(triple.clone());
            }
        } else {
            // For non-timestamp triples, check if they're related to readings in the window
            // We do this by including all triples and then we'll filter
            window_db.add_triple(triple.clone());
        }
    }

    window_db
}

// vibe
fn main() {
    // More readings
    println!("-=-=-=- More readings -=-=-=-");
    let mut window_db = load_dataset("datasets/dataset1_high_readings.nt");
    // let mut window_db = build_window_db(&mut database, 10); // First 10 unique timestamps
    let physical_plan = create_logical_plan(&mut window_db);
    // Execute the plan
    let start = Instant::now();
    let results = physical_plan.execute(&mut window_db);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results, should be 25", results.len());
    dbg!("Physical plan: {:?}", physical_plan);
    // for result in &results {
    //     println!("  {:?}", result);
    // }
    println!();

    // More sensors
    println!("-=-=-=- More sensors -=-=-=-");
    let mut database = load_dataset("datasets/dataset2_high_sensors.nt");
    let mut window_db = build_window_db(&mut database, 10); // First 10 unique timestamps
    let physical_plan = create_logical_plan(&mut window_db);
    // Execute the plan
    let start = Instant::now();
    let results = physical_plan.execute(&mut window_db);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results, should be 50", results.len());
    dbg!("Physical plan: {:?}", physical_plan);
    // for result in &results {
    //     println!("  {:?}", result);
    // }
    println!();
}

/*
use avl::AvlTreeMap;
// using a tree
// initial call: tree = AvlTreeMap::new()
fn reorder_plan_with_tree(logical_plan: &LogicalOperator, tree: &mut AvlTreeMap<i32, i32>, i: i32) {
    dbg!(logical_plan);
    match logical_plan {
        LogicalOperator::Join { left, right } => {
            reorder_plan_with_tree(left, tree, i+1);
            reorder_plan_with_tree(right, tree, 2*i+1);
            tree.insert(i, i);
        }
        _ => ()
    };
}

fn print_tree<K: Debug, D: Debug>(tree: AvlTreeMap<K, D>) {
    println!("Level-order map traversal:");
    println!("{}", tree.len());
    tree.traverse_level_order(|lv, k, v| {
        println!("Level: {}, Key: {:?}, Value: {:?}", lv, k, v);
    });
}
*/