extern crate kolibrie;
use kolibrie::{container_stats::ContainerStats, experiment_logging, rsp::s2r::ContentContainer, rsp_engine::{OperationMode, QueryExecutionMode, RSPBuilder, RSPEngine, ResultConsumer, SimpleR2R}, streamertail_optimizer::{LogicalOperator, PhysicalOperator}};
use shared::triple::Triple;
use std::{fmt, fs::read_to_string, path::Path, sync::{Arc, Mutex}, time::Instant};
use kolibrie::join_reordering;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct StreamEvent {
    stream: String,
    timestamp: usize,
    ntriples: String,
}

#[allow(dead_code)]
fn example_window(path: String, replan_trigger: ReplanTrigger, naief: bool) {
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
    }
}"#;

    set_up_engine(path, query, replan_trigger, naief, None);
}

#[allow(dead_code)]
fn example_window2(path: String, replan_trigger: ReplanTrigger, naief: bool) {
    let query = r#"
    PREFIX ex: <http://example.org/>
    REGISTER RSTREAM <output> AS
    SELECT ?book ?author
    FROM NAMED WINDOW :window1 ON :stream [RANGE 40 STEP 40]
    WHERE {
        WINDOW :window1 {
            ?book ex:writtenBy ?author .
            ?book ex:hasGenre "Romance" .
            ?book ex:wonPrise "True" .
        }
    }
    "#;

    set_up_engine(path, query, replan_trigger, naief, None);
}

pub fn physical_plan_to_string(plan: &PhysicalOperator) -> String {
    format!("{:?}", plan)
}

pub fn are_physical_plans_identical(left: &PhysicalOperator, right: &PhysicalOperator) -> bool {
    physical_plan_to_string(left) == physical_plan_to_string(right)
}

fn set_up_engine(path: String, query: &str, replan_trigger: ReplanTrigger, naief: bool, threshold: Option<f64>) {
    // Set up a file to write results
    let dataset_name = Path::new(&path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("experiment");
    // let run_id = SystemTime::now()
    //     .duration_since(UNIX_EPOCH)
    //     .expect("system clock is before UNIX_EPOCH")
    //     .as_secs();
    let log_file_path = format!(
        "target/experiment_logs/{}_{}.csv",
        dataset_name,
    //    run_id,
        replan_trigger.to_string()
    );

    experiment_logging::init_experiment_log(&log_file_path)
        .expect("failed to initialize experiment log");
    // println!("Logging experiment metrics to {}", log_file_path);

    // Collect results via a shared container that the engine writes into. (just like in rsp_engine_test.rs)
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Standard));

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
    let first_window_iri = window_info.first().unwrap().window_iri.clone();
    let replan_metric = make_replan_fn(replan_trigger);

    // Variable to keep track of the stats of the previous window
    let previous_stats = Arc::new(Mutex::new(ContainerStats::default()));
    let first_window_plan_done = Arc::new(Mutex::new(false));

    // Runtime adaptor: inspect each fired window and optionally swap plan
    engine.set_window_plan_adaptor(
        Arc::new(
        move |window_iri, content: &ContentContainer<Triple>, ts, _current_plan| {

            // START: Gather stats
            let window_start = Instant::now();

            // Get container stats
            let current_stats = ContainerStats::gather_stats(content);

            // Load previous window stats to compare
            let mut previous_stats_guard = previous_stats.lock().unwrap();
            let previous_stats = previous_stats_guard.clone();
            
            // END gather stats
            let stats_time = window_start.elapsed().as_secs_f64() * 1000.0;

            let window_size = content.len();
            // // Take a quick look at the previous stats  
            // println!("Previous stats: Total: {}, Cardinalities: {}, {}, {}",
            //     previous_stats.get_total_triples(),
            //     previous_stats.get_total_subjects(),
            //     previous_stats.get_total_predicates(),
            //     previous_stats.get_total_objects()
            // );

            // Take a quick look at the stats  
            // println!("Current stats: Total: {}, Cardinalities: {}, {}, {}",
            //     current_stats.get_total_triples(),
            //     current_stats.get_total_subjects(),
            //     current_stats.get_total_predicates(),
            //     current_stats.get_total_objects()
            // );
             
            // println!(
            //     "[Adaptor] window={} ts={} tuples_in_window={}",
            //     window_iri, ts, window_size
            // );

            // dbg!(&_current_plan);

            // Check if it's the first time we see this window
            let force_initial_plan = if window_iri == first_window_iri {
                let mut first_window_plan_done_guard = first_window_plan_done.lock().unwrap();
                if !*first_window_plan_done_guard {
                    *first_window_plan_done_guard = true;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            // START new plan calculation
            let start_calculation = Instant::now();

            // Decide whether to replan based on the selected trigger.
            let replan = force_initial_plan || replan_metric(&current_stats, &previous_stats);
            
            // Calculate a potential new plan 
            if replan {
                let new_plan: PhysicalOperator = if (naief || force_initial_plan) {
                    // println!("[Adaptor] Manually calculated initial plan for {}", window_iri);
                    join_reordering::calculate_initial_window_plan(logical_plan.clone(), current_stats.clone())
                } else {
                    //println!("[Adaptor] Recalculate plan for {}", window_iri);
                    join_reordering::recalculate_window_plan(_current_plan.clone(), current_stats.clone())
                };

                // for debugging: check if the new plan is actually different from the current plan
                // if are_physical_plans_identical(_current_plan, &new_plan) {
                //     println!("[Adaptor] The same plan was chosen");
                // } else {
                //     // dbg!(&new_plan);
                //     println!("[Adaptor] New plan was chosen");
                // }

                let optimize_plan_time = start_calculation.elapsed().as_secs_f64() * 1000.0;
                // println!("[Timing] window={} latency={:.3}ms stats_time={:.3}ms tuples={}", 
                //    window_iri, optimize_plan_time, stats_time, window_size);
                if let Err(error) = experiment_logging::append_experiment_row(
                    "optimize",
                    window_iri,
                    ts,
                    optimize_plan_time,
                    Some(stats_time),
                    window_size,
                    None,
                    if force_initial_plan { "initial" } else { "replan" },
                    threshold,
                ) {
                    eprintln!("Failed to write optimization timing for {}: {:?}", window_iri, error);
                }
                *previous_stats_guard = current_stats; // Update previous stats for next window
                return Some(new_plan);
            }
            
            let optimize_plan_time = start_calculation.elapsed().as_secs_f64() * 1000.0;
            // println!("[Timing] window={} latency={:.3}ms stats_time={:.3}ms tuples={}", 
            //     window_iri, optimize_plan_time, stats_time, window_size);
            if let Err(error) = experiment_logging::append_experiment_row(
                "optimize",
                window_iri,
                ts,
                optimize_plan_time,
                Some(stats_time),
                window_size,
                None,
                "no_change",
                threshold,
            ) {
                eprintln!("Failed to write optimization timing for {}: {:?}", window_iri, error);
            }

            // Plan remains the same
            // println!("[Adaptor] Plan remains the same {}", window_iri);
            *previous_stats_guard = current_stats; // Update previous stats for next window

            return None;
        },
    ));

    let amount_of_triples = stream_dataset(&mut engine, &path);
    // println!("Amount of triples: {}", amount_of_triples);

    engine.stop();

    print_engine_results(result_container);
}

fn print_engine_results(result_container: Arc<Mutex<Vec<Vec<(String, String)>>>>) {
    let results = result_container.lock().unwrap();

    // println!("RSP result batches: {}", results.len());
    // for (batch_idx, batch) in results.iter().enumerate() {
    //     if batch_idx > 4 { // only print first 5 batches for readability
    //         break;
    //     }
    //     println!("Batch {} ({} bindings)", batch_idx + 1, batch.len());
    //     for (binding_idx, binding) in batch.iter().enumerate() {
    //         println!("  [{}] {:?}", binding_idx + 1, binding);
    //     }
    // }
}

fn stream_dataset_as_subject_groups(
    engine: &mut RSPEngine<Triple, Vec<(String, String)>>,
    path: &str,
    timestamp_stride: usize,
) -> usize {
    let binding = read_to_string(path).expect("failed to read .nt file");
    let mut timestamp = 0usize;
    let mut current_subject: Option<String> = None;
    let mut amount_of_triples = 0usize;

    for line in binding.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let subject = match line.split_whitespace().next() {
            Some(subject) => subject.to_string(),
            None => continue,
        };

        if current_subject.as_deref() != Some(subject.as_str()) {
            if current_subject.is_some() {
                timestamp += timestamp_stride;
            }
            current_subject = Some(subject);
        }

        let parsed_triples = engine.parse_data(line);
        if parsed_triples.is_empty() {
            continue;
        }

        for triple in parsed_triples {
            engine.add_to_stream("stream", triple, timestamp);
            amount_of_triples += 1;
        }
    }

    amount_of_triples
}

fn stream_dataset(engine: &mut RSPEngine<Triple, Vec<(String, String)>>, path: &str) -> usize {
    match std::path::Path::new(path).extension().and_then(|ext| ext.to_str()) {
        Some("json") | Some("jsonl") | Some("ndjson") => {
            stream_timestamped_events(engine, path)
        }
        _ => stream_dataset_as_subject_groups(engine, path, 1),
    }
}

fn stream_timestamped_events(
    engine: &mut RSPEngine<Triple, Vec<(String, String)>>,
    path: &str,
) -> usize {
    let binding = read_to_string(path).expect("failed to read timestamped event file");
    let mut events = load_stream_events(&binding);
    let mut amount_of_triples = 0usize;

    events.sort_by_key(|event| event.timestamp);

    for event in events {
        if event.stream.trim().is_empty() || event.ntriples.trim().is_empty() {
            continue;
        }

        let parsed_triples = engine.parse_data(&event.ntriples);
        for triple in parsed_triples {
            engine.add_to_stream(&event.stream, triple, event.timestamp);
            amount_of_triples += 1;
        }
    }

    amount_of_triples
}

fn load_stream_events(content: &str) -> Vec<StreamEvent> {
    let trimmed = content.trim_start();

    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed).expect("failed to parse JSON event array")
    } else {
        content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| serde_json::from_str::<StreamEvent>(line).expect("failed to parse JSON event line"))
            .collect()
    }
}

// Use an Arc-wrapped function to choose our replanning trigger
type ReplanFn = Arc<dyn Fn(&ContainerStats, &ContainerStats) -> bool + Send + Sync>;

fn make_replan_fn(trigger: ReplanTrigger) -> ReplanFn {
    match trigger {
        ReplanTrigger::Static => Arc::new(|_, _| false),
        ReplanTrigger::Always => Arc::new(|_, _| true),
        ReplanTrigger::OnSizeChange { threshold } => {
            Arc::new(move |current, previous| current.size_change_ratio(previous) > threshold)
        }
        ReplanTrigger::OnDistributionChange { threshold } => Arc::new(move |current, previous| {
            current.object_distribution_distance(previous) > threshold
        }),
        ReplanTrigger::OnRankingChange { threshold } => {
            Arc::new(move |current, previous| current.object_rank_change_ratio(previous) > threshold)
        }
        ReplanTrigger::Hybrid {
            size_threshold,
            distribution_threshold,
            ranking_threshold,
        } => Arc::new(move |current, previous| {
            current.size_change_ratio(previous) > size_threshold
                || current.object_distribution_distance(previous) > distribution_threshold
                || current.object_rank_change_ratio(previous) > ranking_threshold
        }),
    }
}

#[allow(dead_code)]
enum ReplanTrigger {
    Static,
    Always,
    OnSizeChange { threshold: f64 },
    OnDistributionChange { threshold: f64, },
    OnRankingChange { threshold: f64 },
    Hybrid { size_threshold: f64, distribution_threshold: f64, ranking_threshold: f64 },
}

impl fmt::Display for ReplanTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplanTrigger::Static => write!(f, "Static"),
            ReplanTrigger::Always => write!(f, "Always"),
            ReplanTrigger::OnSizeChange { threshold } => write!(f, "OnSizeChange({})", threshold),
            ReplanTrigger::OnDistributionChange { threshold } => write!(f, "OnDistributionChange"),
            ReplanTrigger::OnRankingChange { threshold } => write!(f, "OnRankingChange" ),
            ReplanTrigger::Hybrid { size_threshold, distribution_threshold, ranking_threshold } => {
                write!(f, "Hybrid({},{},{})", size_threshold, distribution_threshold, ranking_threshold)
            }
        }
    }
}

fn example_window3(path: String, replan_trigger: ReplanTrigger, naief: bool, window_size: usize, threshold: Option<f64>) {
    let query = format!(r#"
    PREFIX ex: <http://example.org/stream/>

    REGISTER RSTREAM <output> AS
    SELECT ?reading ?sensor ?zone ?value
    FROM NAMED WINDOW :window1 ON :stream [RANGE {window_size} STEP {window_size}]
    WHERE {{
    WINDOW :window1 {{
        ?reading <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ex:Reading .
        ?reading ex:fromSensor ?sensor .
        ?sensor ex:locatedIn ?zone .
        ?reading ex:status "ALERT" .
        ?reading ex:value ?value .
    }}
    }}"#);

    set_up_engine(path, &query.to_string(), replan_trigger, naief, threshold);
}

fn experiment(window_size: usize, threshold_dist: f64, threshold_rank: f64, naief: bool) {
    let static_path = "datasets/optimizer_case_static.events.ndjson".to_string();
    let volatile_path = "datasets/optimizer_case_volatile.events.ndjson".to_string();
    let gradual_path = "datasets/optimizer_case_gradual.events.ndjson".to_string();
    // Static data
    example_window3(static_path.clone(), ReplanTrigger::Static, naief, window_size, None);
    example_window3(static_path.clone(), ReplanTrigger::Always, naief, window_size, None);
    example_window3(static_path.clone(), ReplanTrigger::OnDistributionChange { threshold: threshold_dist }, naief, window_size, Some(threshold_dist));
    example_window3(static_path.clone(), ReplanTrigger::OnRankingChange { threshold: threshold_rank }, naief, window_size, Some(threshold_rank));
    // Dynamic data
    example_window3(volatile_path.clone(), ReplanTrigger::Static, naief, window_size, None);
    example_window3(volatile_path.clone(), ReplanTrigger::Always, naief, window_size, None);
    example_window3(volatile_path.clone(), ReplanTrigger::OnDistributionChange { threshold: threshold_dist }, naief, window_size, Some(threshold_dist));
    example_window3(volatile_path.clone(), ReplanTrigger::OnRankingChange { threshold: threshold_rank }, naief, window_size, Some(threshold_rank));
    // Gradual data change
    example_window3(gradual_path.clone(), ReplanTrigger::Static, naief, window_size, None);
    example_window3(gradual_path.clone(), ReplanTrigger::Always, naief, window_size, None);
    example_window3(gradual_path.clone(), ReplanTrigger::OnDistributionChange { threshold: threshold_dist }, naief, window_size, Some(threshold_dist));
    example_window3(gradual_path.clone(), ReplanTrigger::OnRankingChange { threshold: threshold_rank }, naief, window_size, Some(threshold_rank));
}

fn experiment_over_window_size() {
    for window_size in (5..=250).step_by(5) {
        for i in 1..=10 { // do every experiment 10 times
            println!("Run {} for window size {}", i, window_size);
            experiment(window_size, 0.2, 0.3, false);
        }
    }
}

fn experiment_over_thresholds() {
    let window_size = 100;
    for threshold in 1..=50 {
        // threshold goes from 1% to 50% with step of 1%
        let threshold = threshold as f64 / 100.0; 
        for i in 1..=10 { // do every experiment 10 times
            println!("Run {} for threshold {}", i, threshold);
            experiment(window_size, threshold, threshold, false);
        }
    }
}

fn main() {
    // example_window("datasets/dataset_windowed_test.nt".to_string());
    // example_window2("datasets/books.events.ndjson".to_string(), ReplanTrigger::Always);
    // example_window3("datasets/optimizer_case_static.events.ndjson".to_string(), ReplanTrigger::Static);
    // example_window3("datasets/optimizer_case_gradual.events.ndjson".to_string(), ReplanTrigger::OnDistributionChange { threshold: 0.25 });
    // example_window3("datasets/optimizer_case_gradual.events.ndjson".to_string(), ReplanTrigger::OnDistributionChange { threshold: 0.05 });
    // experiment_over_window_size();
    experiment_over_thresholds();
}