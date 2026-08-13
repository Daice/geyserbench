pub use {
    bs58,
    bytes::Bytes,
    futures_util::stream::StreamExt,
    serde::{Deserialize, Serialize},
    std::{
        env,
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
};

mod analysis;
mod backend;
mod config;
mod proto;
mod providers;
mod utils;

use anyhow::{Result, anyhow};
use backend::{BackendStatus, StreamOptions};
use crossbeam_queue::ArrayQueue;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt,
};
use utils::{Comparator, ProgressTracker, get_current_timestamp};
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const DEFAULT_BACKEND_STREAM_URL: &str = "wss://gb.solstack.app/v1/benchmarks/stream";
const MAX_STREAM_TRANSACTIONS: i32 = 100_000;
const SIGNATURE_QUEUE_CAPACITY: usize = 1_024;

struct CliArgs {
    config_path: Option<String>,
    disable_streaming: bool,
    metrics_json_path: Option<String>,
    compare_json_paths: Option<(String, String)>,
}

impl CliArgs {
    fn parse() -> Self {
        let mut args = env::args().skip(1);
        let mut parsed = CliArgs {
            config_path: None,
            disable_streaming: false,
            metrics_json_path: None,
            compare_json_paths: None,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let value = args.next().unwrap_or_else(|| {
                        eprintln!("Missing value for --config");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.config_path = Some(value);
                }
                "--metrics-json" => {
                    let value = args.next().unwrap_or_else(|| {
                        eprintln!("Missing value for --metrics-json");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.metrics_json_path = Some(value);
                }
                "--compare-json" => {
                    let left = args.next().unwrap_or_else(|| {
                        eprintln!("Missing left path for --compare-json");
                        print_usage();
                        std::process::exit(1);
                    });
                    let right = args.next().unwrap_or_else(|| {
                        eprintln!("Missing right path for --compare-json");
                        print_usage();
                        std::process::exit(1);
                    });
                    parsed.compare_json_paths = Some((left, right));
                }
                "--private" => {
                    parsed.disable_streaming = true;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("Unknown argument: {}", other);
                    print_usage();
                    std::process::exit(1);
                }
            }
        }

        if parsed.compare_json_paths.is_some()
            && (parsed.config_path.is_some()
                || parsed.disable_streaming
                || parsed.metrics_json_path.is_some())
        {
            eprintln!(
                "--compare-json cannot be combined with --config, --private, or --metrics-json"
            );
            print_usage();
            std::process::exit(1);
        }

        parsed
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  geyserbench [--config <PATH>] [--private] [--metrics-json <PATH|->]");
    eprintln!("  geyserbench --compare-json <LEFT_JSON> <RIGHT_JSON>");
}

fn display_label(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path)
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact()
        .with_filter(filter_fn(|metadata| {
            !is_sensitive_dependency_log_target(metadata.target())
        }));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()
        .map_err(|err| anyhow!(err.to_string()))?;

    let cli = CliArgs::parse();
    if let Some((left_path, right_path)) = cli.compare_json_paths.as_ref() {
        let left_report = analysis::load_metrics_report(left_path)?;
        let right_report = analysis::load_metrics_report(right_path)?;
        let comparisons = analysis::compare_metrics_reports(&left_report, &right_report)?;
        for comparison in &comparisons {
            analysis::display_metrics_comparison(
                comparison,
                display_label(left_path),
                display_label(right_path),
            );
        }
        return Ok(());
    }

    let config_path = cli.config_path.as_deref().unwrap_or(DEFAULT_CONFIG_PATH);
    let config = config::ConfigToml::load_or_create(config_path)?;
    info!(config_path = config_path, "Loaded configuration");
    let metrics_json_stdout = cli.metrics_json_path.as_deref() == Some("-");
    let primary_endpoints = config
        .endpoint
        .iter()
        .filter(|endpoint| !endpoint.kind.is_preconf())
        .cloned()
        .collect::<Vec<_>>();
    let preconf_endpoint = config
        .endpoint
        .iter()
        .find(|endpoint| endpoint.kind.is_preconf())
        .cloned();

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let start_time_local = get_current_timestamp();
    let comparator = Arc::new(Comparator::new());
    let preconf_comparator = Arc::new(Comparator::new());
    let start_instant = Instant::now();
    let clock_offset_ms: f64;
    let server_started_at_unix_ms: Option<i64>;
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));
    let aborted = Arc::new(AtomicBool::new(false));

    let high_transaction_volume = config.config.transactions > MAX_STREAM_TRANSACTIONS;
    if high_transaction_volume {
        warn!(
            transactions = config.config.transactions,
            threshold = MAX_STREAM_TRANSACTIONS,
            "Disabling backend streaming for high-volume run; backend streaming is unavailable when transactions exceed the threshold"
        );
    }

    let mut backend_settings = config.backend.clone();
    backend_settings.enabled = !(cli.disable_streaming || high_transaction_volume);
    backend_settings.url = Some(DEFAULT_BACKEND_STREAM_URL.to_string());

    let mut backend_handle = None;
    let mut signature_queues: Option<Vec<Arc<ArrayQueue<backend::SignatureEnvelope>>>> = None;
    let mut signature_forwarder: Option<thread::JoinHandle<()>> = None;
    let mut forwarder_stop: Option<Arc<AtomicBool>> = None;
    let mut backend_run_id = None;

    if backend_settings.enabled && false {
        let url = backend_settings
            .url
            .clone()
            .ok_or_else(|| anyhow!("backend streaming enabled but no URL configured"))?;
        let options = StreamOptions { url, summary: None };
        let handle = backend::connect_stream(options, &config.config, &primary_endpoints).await?;
        clock_offset_ms = handle.clock_offset_ms();
        server_started_at_unix_ms = handle.server_started_at_unix_ms();
        let run_id = handle.run_id().to_string();
        info!(
            run_id = %run_id,
            started_at_unix_ms = server_started_at_unix_ms,
            clock_offset_ms,
            "Streaming backend session initialised"
        );
        backend_run_id = Some(run_id.clone());

        let mut queues = Vec::with_capacity(primary_endpoints.len());
        for _ in 0..primary_endpoints.len() {
            queues.push(Arc::new(ArrayQueue::new(SIGNATURE_QUEUE_CAPACITY)));
        }
        let queue_handles = queues.iter().map(Arc::clone).collect::<Vec<_>>();
        signature_queues = Some(queues);

        let mut status_rx = handle.status();
        let shutdown_for_backend = shutdown_tx.clone();
        let run_id_for_status = run_id.clone();
        tokio::spawn(async move {
            while status_rx.changed().await.is_ok() {
                match status_rx.borrow().clone() {
                    BackendStatus::Failed { message } => {
                        error!(run_id = %run_id_for_status, error = %message, "Backend streaming failed");
                        let _ = shutdown_for_backend.send(());
                        break;
                    }
                    BackendStatus::Completed { .. } => break,
                    BackendStatus::Ready { run_id } => {
                        debug!(run_id = %run_id, "Backend stream ready");
                    }
                    BackendStatus::Initializing => {}
                }
            }
        });

        let backend_sender = handle.signature_sender();
        let run_id_for_forwarder = run_id.clone();
        let stop_flag = Arc::new(AtomicBool::new(false));
        forwarder_stop = Some(stop_flag.clone());
        let forwarder = thread::spawn(move || {
            let queue_handles = queue_handles;
            loop {
                let mut did_work = false;
                for queue in &queue_handles {
                    while let Some(envelope) = queue.pop() {
                        did_work = true;
                        if backend_sender.blocking_send(envelope).is_err() {
                            warn!(run_id = %run_id_for_forwarder, "Failed to forward signature to backend");
                            return;
                        }
                    }
                }

                let should_stop = stop_flag.load(Ordering::Acquire);
                if should_stop && queue_handles.iter().all(|queue| queue.is_empty()) {
                    break;
                }

                if !did_work {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        signature_forwarder = Some(forwarder);
        backend_handle = Some(handle);
    } else {
        info!("Backend streaming disabled; collecting metrics locally");
    }

    let mut handles = Vec::new();
    let endpoint_names: Vec<String> = primary_endpoints
        .iter()
        .map(|endpoint| endpoint.name.clone())
        .collect();
    let yellowstone_endpoint_names: Vec<String> = config
        .endpoint
        .iter()
        .filter(|endpoint| endpoint.kind.is_yellowstone_family())
        .map(|endpoint| endpoint.name.clone())
        .collect();
    let global_target = if config.config.transactions > 0 {
        Some(config.config.transactions as usize)
    } else {
        None
    };
    let progress_tracker = global_target.map(|target| Arc::new(ProgressTracker::new(target)));

    let total_producers = primary_endpoints.len();
    let mut primary_index = 0usize;
    let endpoints_to_spawn = preconf_endpoint
        .iter()
        .cloned()
        .chain(primary_endpoints.iter().cloned());
    for endpoint in endpoints_to_spawn {
        let provider = providers::create_provider(&endpoint.kind);
        let shared_config = config.config.clone();
        let is_preconf = endpoint.kind.is_preconf();
        let signature_queue = if is_preconf {
            None
        } else {
            let queue = signature_queues
                .as_ref()
                .and_then(|queues| queues.get(primary_index).cloned());
            primary_index += 1;
            queue
        };
        let context = providers::ProviderContext {
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx: shutdown_tx.subscribe(),
            start_wallclock_secs: start_time_local,
            start_instant,
            comparator: if is_preconf {
                preconf_comparator.clone()
            } else {
                comparator.clone()
            },
            signature_tx: signature_queue,
            shared_counter: shared_counter.clone(),
            shared_shutdown: shared_shutdown.clone(),
            target_transactions: global_target,
            total_producers,
            progress: progress_tracker.clone(),
        };

        handles.push((
            is_preconf,
            provider.process(endpoint, shared_config, context),
        ));
    }

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let shared_shutdown = shared_shutdown.clone();
        let aborted = aborted.clone();
        async move {
            match ctrl_c().await {
                Ok(()) => {
                    let already_aborting = aborted.swap(true, Ordering::AcqRel);
                    if already_aborting {
                        info!("Received additional Ctrl+C; shutdown already in progress");
                    } else {
                        info!("Received Ctrl+C; initiating shutdown");
                    }
                    shared_shutdown.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(());
                }
                Err(err) => error!(error = %err, "Failed to listen for Ctrl+C"),
            }
        }
    });

    let mut preconf_failure = None;
    for (is_preconf, handle) in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!(error = ?e, "Provider task returned error");
                if is_preconf {
                    preconf_failure = Some(e.to_string());
                }
            }
            Err(e) => {
                error!(error = ?e, "Provider join error");
                if is_preconf {
                    preconf_failure = Some(e.to_string());
                }
            }
        }
    }

    let preconf_not_ready = preconf_endpoint.is_some() && !preconf_comparator.is_ready();
    if preconf_not_ready && preconf_failure.is_none() && !aborted.load(Ordering::Acquire) {
        error!("Helius preconf subscription was not acknowledged before the run ended");
    }
    let run_aborted =
        aborted.load(Ordering::Acquire) || preconf_failure.is_some() || preconf_not_ready;

    let run_summary = if !run_aborted {
        let mut summary = analysis::compute_run_summary(
            comparator.as_ref(),
            &endpoint_names,
            &yellowstone_endpoint_names,
        );
        if let Some(endpoint) = preconf_endpoint.as_ref() {
            summary.helius_preconf = Some(analysis::compute_helius_preconf_summary(
                comparator.as_ref(),
                preconf_comparator.as_ref(),
                &primary_endpoints,
                &endpoint.name,
            ));
        }
        Some(summary)
    } else {
        None
    };

    if let Some(handle) = backend_handle {
        if run_aborted {
            if let Some(run_id) = backend_run_id.as_ref() {
                info!(run_id = %run_id, "Skipping backend finalisation due to user abort");
            } else {
                info!("Skipping backend finalisation due to user abort");
            }
            // Dropping the handle without calling finish() prevents the backend run from being saved.
        } else {
            let run_id = backend_run_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            match handle.finish().await {
                Ok(result) => {
                    info!(run_id = %run_id, "Backend completed run");
                    debug!(run_id = %run_id, response = %result.response, "Backend completion payload");
                }
                Err(err) => {
                    error!(run_id = %run_id, error = %err, "Backend streaming ended with error");
                }
            }
        }
    }

    let _ = signature_queues.take();
    if let Some(stop) = forwarder_stop.as_ref() {
        stop.store(true, Ordering::Release);
    }

    if let Some(join) = signature_forwarder
        && let Err(err) = join.join()
    {
        warn!(
            "Signature forwarder thread terminated unexpectedly: {:?}",
            err
        );
    }

    let run_finished_at_unix_secs = get_current_timestamp();
    let metrics_report = if !run_aborted {
        run_summary.as_ref().map(|summary| {
            analysis::build_metrics_report(
                summary,
                comparator.as_ref(),
                &config.config,
                &primary_endpoints,
                start_time_local,
                run_finished_at_unix_secs,
            )
        })
    } else {
        None
    };

    if !run_aborted {
        if let Some(summary) = run_summary.as_ref() {
            if !metrics_json_stdout {
                analysis::display_run_summary(summary);
            }
        }

        if let Some(report) = metrics_report.as_ref() {
            let metrics_json = serde_json::to_string(report)
                .map_err(|err| anyhow!("failed to serialize metrics report: {err}"))?;
            debug!(metrics = %metrics_json, "Computed run metrics");
        }

        if let (Some(path), Some(report)) =
            (cli.metrics_json_path.as_deref(), metrics_report.as_ref())
        {
            analysis::write_metrics_report(report, path)?;
        }

        if let Some(run_id) = backend_run_id
            && !metrics_json_stdout
        {
            println!("🔗 Share this benchmark run: https://runs.solstack.app/run/{run_id}");
        }
    } else {
        info!("Benchmark aborted before completion; no results were generated");
    }

    if let Some(message) = preconf_failure {
        return Err(anyhow!("Helius preconf provider failed: {message}"));
    }
    if preconf_not_ready && !aborted.load(Ordering::Acquire) {
        return Err(anyhow!(
            "Helius preconf subscription was not acknowledged before the run ended"
        ));
    }

    Ok(())
}

fn is_sensitive_dependency_log_target(target: &str) -> bool {
    target == "tungstenite::handshake::client"
        || target.starts_with("tungstenite::handshake::client::")
}

#[cfg(test)]
mod tests {
    use super::is_sensitive_dependency_log_target;

    #[test]
    fn identifies_tungstenite_handshake_logs_that_can_contain_api_keys() {
        assert!(is_sensitive_dependency_log_target(
            "tungstenite::handshake::client"
        ));
        assert!(is_sensitive_dependency_log_target(
            "tungstenite::handshake::client::tests"
        ));
        assert!(!is_sensitive_dependency_log_target("tungstenite::protocol"));
    }
}
