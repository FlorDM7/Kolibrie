extern crate kolibrie;
use kolibrie::{execute_query::{execute_query, parse_sparql_to_logical_plan}, rsp_engine::{OperationMode, QueryExecutionMode, RSPBuilder, RSPEngine, ResultConsumer, SimpleR2R}, sparql_database::*, streamertail_optimizer::{DatabaseStats, build_logical_plan}};
use shared::triple::Triple;
use std::{fs::read_to_string, sync::{Arc, Mutex}, time::Instant};
use kolibrie::join_reordering;

fn example_static(path: String) {
    // create empty database
    let mut db = SparqlDatabase::new();

    // read file with turtle data
    let binding = read_to_string(path).expect("failed to read .nt file");
    let turtle_data = binding.as_str();

    // parse N-Triples data
    db.parse_ntriples_and_add(turtle_data);

    // get stats from data
    let stats = DatabaseStats::gather_stats_fast(&db);

    println!("Total: {}", stats.total_triples);

    // sparql query
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
    // create empty database
    // let mut db = SparqlDatabase::new();

    // read file with turtle data
    // let binding = read_to_string(path).expect("failed to read .nt file");
    // let turtle_data = binding.as_str();

    // parse N-Triples data
    // db.parse_ntriples_and_add(turtle_data);

    // sparql query
    let rsp_ql_query = r#"  PREFIX ex: <http://example.org/>
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

    // Collect results via a shared container that the engine writes into.
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        // println!("Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
            .expect("Failed to build RSP engine");

    // Print per window physical query plan
    let window_info = engine.get_window_info();
    let query_plan = engine.get_query_plan();
    println!("Per-window physical plans:");
    for (idx, window) in window_info.iter().enumerate() {
        if let Some(plan) = query_plan.window_plans.get(idx) {
            println!("  {} -> {:?}", window.window_iri, plan);
        }
    }

    // small hack to make sure the encoder is aligned between parsing and query injection
    // engine.parse_data("a a <http://www.w3.org/test/SuperType> .");

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
    
    // TODO use my optimizer
    // let physical_plan = join_reordering::naive_reordering(logical_plan, &mut db);
}

fn main() {
    example_window("datasets/dataset_windowed_test.nt".to_string());
    // example_static("datasets/dataset1_complete.nt".to_string());
    // example_static("datasets/dataset2_high_sensors.nt".to_string());
}