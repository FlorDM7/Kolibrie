use std::fs::{create_dir_all, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

static LOG_FILE_PATH: OnceLock<Mutex<PathBuf>> = OnceLock::new();

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

    if let Some(lock) = LOG_FILE_PATH.get() {
        let mut current_path = lock.lock().map_err(|_| io::Error::other("failed to lock experiment log path"))?;
        *current_path = path;
        Ok(())
    } else {
        LOG_FILE_PATH
            .set(Mutex::new(path))
            .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "experiment log already initialized"))
    }
}

fn current_log_path() -> PathBuf {
    LOG_FILE_PATH
        .get()
        .and_then(|lock| lock.lock().ok().map(|path| path.clone()))
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
