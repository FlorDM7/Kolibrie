extern crate kolibrie;
use kolibrie::{execute_query::execute_query, sparql_database::*, streamertail_optimizer::DatabaseStats};
use std::fs::read_to_string;

fn example_temperature_sensors(path: String) {
    // create empty database
    let mut db = SparqlDatabase::new();

    // read file with turtle data
    let file = read_to_string(path);
    let binding = file.unwrap();
    let turtle_data = binding.as_str();

    // parse N-Triples data
    db.parse_ntriples_and_add(turtle_data);

    // get stats from data
    let _stats = DatabaseStats::gather_stats_fast(&db);

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
                            }"#;

    //  FILTER(?value > 25) . does not work

    let results = execute_query(sparql, &mut db);
    let size = results.len();

    println!("Results {}", size);
    
}

fn main() {
    example_temperature_sensors("datasets/dataset1_complete.nt".to_string());
    // example_temperature_sensors("datasets/dataset2_high_sensors.nt".to_string());
}