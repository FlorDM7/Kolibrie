extern crate kolibrie;
use kolibrie::{container_stats::ContainerStats, execute_query::parse_sparql_to_logical_plan, rsp::s2r::ContentContainer, rsp_engine::{OperationMode, QueryExecutionMode, RSPBuilder, RSPEngine, ResultConsumer, SimpleR2R}, sparql_database::*, streamertail_optimizer::{DatabaseStats, LogicalOperator}};
use shared::triple::Triple;
use std::{fs::read_to_string, sync::{Arc, Mutex}, time::Instant};
use kolibrie::join_reordering;

fn example_static(path: String) {
    // Create empty database
    let mut db = SparqlDatabase::new();

    // Read file with turtle data
    let binding = read_to_string(path).expect("failed to read .nt file");
    let turtle_data = binding.as_str();

    // Parse N-Triples data
    db.parse_ntriples_and_add(turtle_data);

    // Get stats from data
    let stats = DatabaseStats::gather_stats_fast(&db);

    println!("Total: {}", stats.total_triples);

    // SPARQL query
    let sparql = r#"  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                            PREFIX ex: <http://example.org/>
                            SELECT ?sensor ?location ?reading ?timestamp
                            WHERE {
                                ?sensor rdf:type ex:TemperatureSensor .
                                ?sensor ex:locatedIn ?location .
                                ?sensor ex:hasReading ?reading .
                                ?reading ex:value ?value .
                                ?reading ex:timestamp ?timestamp .
                                FILTER(?value > 25) .
                            }"#;

    let logical_plan = parse_sparql_to_logical_plan(sparql, &mut db).expect("Parse went wrong");

    let physical_plan = join_reordering::naive_reordering(logical_plan, &mut db);

    let start = Instant::now();
    let results = physical_plan.execute(&mut db);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results, should be 250", results.len());
}

fn example_window(path: String) {
    // SPARQL query
    let query = r#"  PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

REGISTER RSTREAM <output> AS
SELECT ?sensor ?location ?reading ?timestamp ?value
FROM NAMED WINDOW :window1 ON :stream [RANGE PT1H STEP PT1H]
WHERE {
    WINDOW :window1 {
        ?sensor rdf:type ex:TemperatureSensor .
        ?sensor ex:locatedIn ?location .
        ?sensor ex:hasReading ?reading .
        ?reading ex:value ?value .
        ?reading ex:timestamp ?timestamp .
    }
    FILTER(?value > 25)
}"#;

    // Collect results via a shared container that the engine writes into. (just like in rsp_engine_test.rs)
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
            .add_rsp_ql_query(query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .set_operation_mode(OperationMode::SingleThread)
            .build()
            .expect("Failed to build RSP engine");

    // View logical plan per window
    let window_info = engine.get_window_info();
    let window_plans: Vec<LogicalOperator> = window_info.iter().map(|w| {
        w.query.clone()
    }).collect();

    // Select logical plan of the first (and only) window
    let logical_plan = window_plans.first().unwrap().clone();

    // Variable to keep track of the stats of the previous window
    let previous_stats = Arc::new(Mutex::new(ContainerStats::default()));

    // Runtime adaptor: inspect each fired window and optionally swap plan
    engine.set_window_plan_adaptor(
        Arc::new(
        move |window_iri, content: &ContentContainer<Triple>, ts, _current_plan| {
            
            let window_size = content.len();
            println!(
                "[Adaptor] window={} ts={} tuples_in_window={}",
                window_iri, ts, window_size
            );

            // Get container stats
            let current_stats = ContainerStats::gather_stats(content);

            let mut previous_stats_guard = previous_stats.lock().unwrap();

            // Take a quick look at the previous stats  
            println!("Previous stats: Total: {}, Cardinalities: {}, {}, {}",
                previous_stats_guard.get_total_triples(),
                previous_stats_guard.get_total_subjects(),
                previous_stats_guard.get_total_predicates(),
                previous_stats_guard.get_total_objects()
            );

            // Take a quick look at the stats  
            println!("Current stats: Total: {}, Cardinalities: {}, {}, {}",
                current_stats.get_total_triples(),
                current_stats.get_total_subjects(),
                current_stats.get_total_predicates(),
                current_stats.get_total_objects()
            );

            // Calculate a potential new plan 
            if true { // only do this under a certain condition (TBD)
                let new_plan = join_reordering::pick_some_plan(logical_plan.clone());
                println!("[Adaptor] Recalculate plan for {}", window_iri);
                println!("{:?}", new_plan);
                *previous_stats_guard = current_stats; // Update previous stats for next window
                return Some(new_plan);
            }
            
            // Plan remains the same
            println!("[Adaptor] Plan remains the same {}", window_iri);
            *previous_stats_guard = current_stats; // Update previous stats for next window
            return None;
        },
    ));

    // Add data to stream with increasing event time.
    // With RANGE PT1H and default ON_WINDOW_CLOSE, timestamps must advance
    // beyond the window close boundary to emit results.
    let binding = read_to_string(path).expect("failed to read .nt file");
    let data = binding.as_str();
    let triples = engine.parse_data(&data);
    println!("Amount of triples: {}", triples.len());
    for (i, triple) in triples.into_iter().enumerate() {
        let ts = i*60;
        engine.add_to_stream("stream", triple, ts);
    }

    engine.stop();

    let results = result_container.lock().unwrap();

    println!("RSP result batches: {}", results.len());
}

fn main() {
    example_window("datasets/dataset_windowed_test.nt".to_string());
    // example_static("datasets/dataset1_complete.nt".to_string());
    // example_static("datasets/dataset2_high_sensors.nt".to_string());
}