use std::fs::{create_dir_all, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::OnceLock;

static LOG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

pub fn init_experiment_log(file_path: impl Into<PathBuf>) -> io::Result<()> {
    let path = file_path.into();

    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;

    writeln!(file, "phase,window,ts,elapsed_ms,aux_ms,tuples,results,note")?;

    LOG_FILE_PATH
        .set(path)
        .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "experiment log already initialized"))
}

fn current_log_path() -> PathBuf {
    LOG_FILE_PATH
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("target/experiment_logs/kolibrie_experiments.csv"))
}

pub fn append_experiment_row(
    phase: &str,
    window: &str,
    ts: usize,
    elapsed_ms: f64,
    aux_ms: Option<f64>,
    tuples: usize,
    results: Option<usize>,
    note: &str,
) -> io::Result<()> {
    let path = current_log_path();

    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let aux_ms = aux_ms.map(|value| format!("{:.6}", value)).unwrap_or_default();
    let results = results.map(|value| value.to_string()).unwrap_or_default();

    writeln!(
        file,
        "{},{},{},{:.6},{},{},{},{}",
        phase,
        window,
        ts,
        elapsed_ms,
        aux_ms,
        tuples,
        results,
        note,
    )?;

    file.flush()
}
