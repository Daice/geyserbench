use crate::{
    config::{Config, Endpoint, EndpointKind},
    utils::{Comparator, TransactionData, percentile},
};
use anyhow::{Context, Result, bail};
use comfy_table::{ContentArrangement, Table};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    fs,
    time::Duration,
};

#[cfg(target_os = "windows")]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::ASCII_FULL
}

#[cfg(not(target_os = "windows"))]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::UTF8_FULL
}

#[derive(Default)]
pub struct EndpointStats {
    pub total_observations: usize,
    pub first_detections: usize,
    pub delays_ms: Vec<f64>,
    pub backfill_transactions: usize,
}

#[derive(Default)]
struct YellowstoneCreatedAtEndpointStats {
    deltas_ms: Vec<f64>,
}

#[derive(Default)]
struct YellowstoneEndpointLocalStats {
    observed_signatures: usize,
    live_observations: usize,
    eligible_created_at: usize,
    missing_created_at: usize,
    zero_created_at: usize,
    backfill_transactions: usize,
    deltas_ms: Vec<f64>,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointSummary {
    pub name: String,
    pub first_share: f64,
    pub p50_delay_ms: Option<f64>,
    pub p95_delay_ms: Option<f64>,
    pub p99_delay_ms: Option<f64>,
    pub valid_transactions: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct YellowstoneCreatedAtEndpointSummary {
    pub name: String,
    pub avg_delta_ms: Option<f64>,
    pub p50_delta_ms: Option<f64>,
    pub p95_delta_ms: Option<f64>,
    pub p99_delta_ms: Option<f64>,
}

#[derive(Debug, Default, Clone)]
pub struct YellowstoneCreatedAtSummary {
    pub eligible_signatures: usize,
    pub endpoints: Vec<YellowstoneCreatedAtEndpointSummary>,
    pub has_data: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct YellowstoneEndpointLocalSummary {
    pub observed_signatures: usize,
    pub live_observations: usize,
    pub eligible_created_at: usize,
    pub missing_created_at: usize,
    pub zero_created_at: usize,
    pub eligible_ratio: Option<f64>,
    pub zero_created_at_rate: Option<f64>,
    pub backfill_rate: Option<f64>,
    pub raw_p50_delta_ms: Option<f64>,
    pub raw_p95_delta_ms: Option<f64>,
    pub raw_p99_delta_ms: Option<f64>,
    pub jitter_p95_minus_p50_ms: Option<f64>,
    pub jitter_p99_minus_p50_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub yellowstone_created_at: Option<YellowstoneCreatedAtSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsReport {
    pub account: Vec<String>,
    pub commitment: String,
    pub run_started_at_unix_ms: f64,
    pub run_finished_at_unix_ms: f64,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
    pub per_endpoint: BTreeMap<String, MetricsEndpointReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yellowstone_created_at: Option<MetricsYellowstoneCreatedAtReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsEndpointReport {
    pub endpoint_url: String,
    pub endpoint_kind: String,
    pub first_detection_rate: f64,
    pub p50_latency_ms: Option<f64>,
    pub p95_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
    pub observations: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yellowstone_endpoint_local: Option<YellowstoneEndpointLocalSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsYellowstoneCreatedAtReport {
    pub eligible_signatures: usize,
    pub per_endpoint: BTreeMap<String, YellowstoneCreatedAtEndpointSummary>,
}

#[derive(Debug, Clone)]
pub struct MetricsComparison {
    pub endpoint_url: String,
    pub endpoint_kind: String,
    pub account: Vec<String>,
    pub commitment: String,
    pub reliable_for_latency: bool,
    pub unreliability_reasons: Vec<String>,
    pub stability_winner: Option<ComparisonWinner>,
    pub data_quality_winner: ComparisonWinner,
    pub left: MetricsComparisonSide,
    pub right: MetricsComparisonSide,
}

#[derive(Debug, Clone)]
pub struct MetricsComparisonSide {
    pub endpoint_name: String,
    pub endpoint_metrics: MetricsEndpointReport,
    pub local_summary: YellowstoneEndpointLocalSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonWinner {
    Left,
    Right,
    Tie,
}

impl ComparisonWinner {
    fn label(self, left_label: &str, right_label: &str) -> String {
        match self {
            Self::Left => left_label.to_string(),
            Self::Right => right_label.to_string(),
            Self::Tie => "Tie".to_string(),
        }
    }
}

pub fn compute_run_summary(
    comparator: &Comparator,
    endpoint_names: &[String],
    yellowstone_endpoint_names: &[String],
) -> RunSummary {
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();
    let expected_producers = endpoint_names.len();
    let mut total_signatures = 0usize;
    let mut backfill_signatures = 0usize;

    for endpoint_name in endpoint_names {
        endpoint_stats.insert(endpoint_name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if expected_producers > 0 && sig_data.len() != expected_producers {
            // Skip partial observations to mirror backend results
            continue;
        }

        let is_historical = sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs);

        if is_historical {
            backfill_signatures += 1;
            for endpoint in sig_data.keys() {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.backfill_transactions += 1;
                }
            }
            continue;
        }

        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };

        total_signatures += 1;
        let first_endpoint_name = first_endpoint.clone();

        for (endpoint, tx) in sig_data.iter() {
            if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                stats.total_observations += 1;
                if endpoint == &first_endpoint_name {
                    stats.first_detections += 1;
                    stats.delays_ms.push(0.0);
                } else {
                    let delay_ms = diff_ms(tx, first_tx).max(0.0);
                    stats.delays_ms.push(delay_ms);
                }
            }
        }
    }

    let endpoints: Vec<EndpointSummary> = endpoint_stats
        .into_iter()
        .map(|(endpoint, stats)| build_summary(endpoint, stats, total_signatures))
        .collect();

    let has_data = total_signatures > 0;

    let fastest_endpoint = endpoints
        .iter()
        .filter(|summary| summary.valid_transactions > 0)
        .min_by(|a, b| compare_latency(a, b))
        .map(|summary| summary.name.clone());

    let yellowstone_created_at =
        compute_yellowstone_created_at_summary(comparator, yellowstone_endpoint_names);

    RunSummary {
        endpoints,
        yellowstone_created_at,
        fastest_endpoint,
        has_data,
        total_signatures,
        backfill_signatures,
    }
}

pub fn display_run_summary(summary: &RunSummary) {
    println!("\nFinished test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
    } else {
        let fastest_name_ref = summary.fastest_endpoint.as_deref();
        let mut summary_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
        summary_rows.sort_by(|a, b| compare_latency(a, b));

        for summary in summary_rows {
            if summary.valid_transactions == 0 {
                println!("{}: Not enough data", summary.name);
                continue;
            }

            let raw_win_rate = format_percent(summary.first_share);
            let win_rate = if raw_win_rate == "—" {
                raw_win_rate
            } else {
                format!("{}%", raw_win_rate)
            };
            let is_fastest = fastest_name_ref == Some(summary.name.as_str());

            if is_fastest {
                println!(
                    "{}: Win rate {}, p50 0.00ms (fastest)",
                    summary.name, win_rate,
                );
            } else {
                let p50_delay = summary
                    .p50_delay_ms
                    .map(|v| format!("{v:.2}ms"))
                    .unwrap_or_else(|| "—".to_string());
                println!("{}: Win rate {}, p50 {}", summary.name, win_rate, p50_delay);
            }
        }
    }

    println!("\nDetailed test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
    } else {
        let mut table_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
        table_rows.sort_by(|a, b| compare_latency(a, b));

        let mut table = Table::new();
        table.load_preset(table_preset());
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            "Endpoint", "First %", "P50 ms", "P95 ms", "P99 ms", "Valid Tx", "Firsts", "Backfill",
        ]);

        for summary in table_rows {
            table.add_row(vec![
                summary.name.clone(),
                format_percent(summary.first_share),
                format_latency_value(summary.p50_delay_ms),
                format_latency_value(summary.p95_delay_ms),
                format_latency_value(summary.p99_delay_ms),
                summary.valid_transactions.to_string(),
                summary.first_detections.to_string(),
                summary.backfill_transactions.to_string(),
            ]);
        }

        println!("{table}");
    }

    if let Some(yellowstone_summary) = summary.yellowstone_created_at.as_ref() {
        display_yellowstone_created_at_summary(yellowstone_summary);
    }
}

pub fn build_metrics_report(
    summary: &RunSummary,
    comparator: &Comparator,
    run_config: &Config,
    endpoints: &[Endpoint],
    run_started_at_unix_secs: f64,
    run_finished_at_unix_secs: f64,
) -> MetricsReport {
    let endpoint_summaries: HashMap<&str, &EndpointSummary> = summary
        .endpoints
        .iter()
        .map(|endpoint| (endpoint.name.as_str(), endpoint))
        .collect();
    let local_summaries = compute_yellowstone_endpoint_local_summaries(comparator, endpoints);

    let mut per_endpoint = BTreeMap::new();
    for endpoint in endpoints {
        let summary = endpoint_summaries.get(endpoint.name.as_str());
        per_endpoint.insert(
            endpoint.name.clone(),
            MetricsEndpointReport {
                endpoint_url: endpoint.url.clone(),
                endpoint_kind: endpoint.kind.as_str().to_string(),
                first_detection_rate: summary.map_or(0.0, |value| value.first_share),
                p50_latency_ms: summary.and_then(|value| value.p50_delay_ms),
                p95_latency_ms: summary.and_then(|value| value.p95_delay_ms),
                p99_latency_ms: summary.and_then(|value| value.p99_delay_ms),
                observations: summary.map_or(0, |value| value.valid_transactions),
                first_detections: summary.map_or(0, |value| value.first_detections),
                backfill_transactions: summary.map_or(0, |value| value.backfill_transactions),
                yellowstone_endpoint_local: local_summaries.get(&endpoint.name).cloned(),
            },
        );
    }

    let yellowstone_created_at = summary.yellowstone_created_at.as_ref().map(|payload| {
        let per_endpoint = payload
            .endpoints
            .iter()
            .cloned()
            .map(|endpoint| (endpoint.name.clone(), endpoint))
            .collect();

        MetricsYellowstoneCreatedAtReport {
            eligible_signatures: payload.eligible_signatures,
            per_endpoint,
        }
    });

    MetricsReport {
        account: run_config.account.clone(),
        commitment: run_config.commitment.as_str().to_string(),
        run_started_at_unix_ms: run_started_at_unix_secs * 1_000.0,
        run_finished_at_unix_ms: run_finished_at_unix_secs * 1_000.0,
        total_signatures: summary.total_signatures,
        backfill_signatures: summary.backfill_signatures,
        per_endpoint,
        yellowstone_created_at,
    }
}

pub fn write_metrics_report(report: &MetricsReport, path: &str) -> Result<()> {
    let payload =
        serde_json::to_string_pretty(report).context("failed to serialize metrics report")?;

    if path == "-" {
        println!("{payload}");
        return Ok(());
    }

    fs::write(path, payload)
        .with_context(|| format!("failed to write metrics report to {path}"))?;
    Ok(())
}

pub fn load_metrics_report(path: &str) -> Result<MetricsReport> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read metrics report {path}"))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse metrics report {path}"))
}

pub fn compare_metrics_reports(
    left: &MetricsReport,
    right: &MetricsReport,
) -> Result<MetricsComparison> {
    let left_side = select_comparable_endpoint(left)?;
    let right_side = select_comparable_endpoint(right)?;

    if left_side.endpoint_metrics.endpoint_url != right_side.endpoint_metrics.endpoint_url {
        bail!(
            "metrics reports target different endpoint_url values: '{}' vs '{}'",
            left_side.endpoint_metrics.endpoint_url,
            right_side.endpoint_metrics.endpoint_url
        );
    }
    if left_side.endpoint_metrics.endpoint_kind != right_side.endpoint_metrics.endpoint_kind {
        bail!(
            "metrics reports target different endpoint_kind values: '{}' vs '{}'",
            left_side.endpoint_metrics.endpoint_kind,
            right_side.endpoint_metrics.endpoint_kind
        );
    }
    if left.account != right.account {
        bail!("metrics reports use different account filters");
    }
    if left.commitment != right.commitment {
        bail!(
            "metrics reports use different commitment levels: '{}' vs '{}'",
            left.commitment,
            right.commitment
        );
    }

    let mut unreliability_reasons = Vec::new();
    collect_reliability_issues("left", &left_side.local_summary, &mut unreliability_reasons);
    collect_reliability_issues(
        "right",
        &right_side.local_summary,
        &mut unreliability_reasons,
    );

    let reliable_for_latency = unreliability_reasons.is_empty();
    let stability_winner = reliable_for_latency.then(|| {
        ordering_to_winner(compare_stability(
            &left_side.local_summary,
            &right_side.local_summary,
        ))
    });
    let data_quality_winner = ordering_to_winner(compare_quality(
        &left_side.local_summary,
        &right_side.local_summary,
    ));

    Ok(MetricsComparison {
        endpoint_url: left_side.endpoint_metrics.endpoint_url.clone(),
        endpoint_kind: left_side.endpoint_metrics.endpoint_kind.clone(),
        account: left.account.clone(),
        commitment: left.commitment.clone(),
        reliable_for_latency,
        unreliability_reasons,
        stability_winner,
        data_quality_winner,
        left: left_side,
        right: right_side,
    })
}

pub fn display_metrics_comparison(
    comparison: &MetricsComparison,
    left_label: &str,
    right_label: &str,
) {
    println!("\nAsync Yellowstone comparison");
    println!("--------------------------------------------");
    println!("Endpoint URL: {}", comparison.endpoint_url);
    println!("Endpoint kind: {}", comparison.endpoint_kind);
    println!("Commitment: {}", comparison.commitment);
    println!("Accounts: {}", comparison.account.join(", "));

    if comparison.reliable_for_latency {
        if let Some(winner) = comparison.stability_winner {
            println!(
                "Stability winner: {}",
                winner.label(left_label, right_label)
            );
        }
    } else {
        println!(
            "Latency comparison unreliable: {}",
            comparison.unreliability_reasons.join("; ")
        );
    }

    println!(
        "Data quality winner: {}",
        comparison
            .data_quality_winner
            .label(left_label, right_label)
    );
    println!("Note: raw delta metrics are host-clock biased and shown for reference only.");

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["Metric", left_label, right_label]);

    for (label, left_value, right_value) in [
        (
            "Endpoint name",
            comparison.left.endpoint_name.clone(),
            comparison.right.endpoint_name.clone(),
        ),
        (
            "Eligible created_at",
            comparison
                .left
                .local_summary
                .eligible_created_at
                .to_string(),
            comparison
                .right
                .local_summary
                .eligible_created_at
                .to_string(),
        ),
        (
            "Eligible ratio",
            format_ratio(comparison.left.local_summary.eligible_ratio),
            format_ratio(comparison.right.local_summary.eligible_ratio),
        ),
        (
            "Missing created_at",
            comparison.left.local_summary.missing_created_at.to_string(),
            comparison
                .right
                .local_summary
                .missing_created_at
                .to_string(),
        ),
        (
            "Zero created_at",
            comparison.left.local_summary.zero_created_at.to_string(),
            comparison.right.local_summary.zero_created_at.to_string(),
        ),
        (
            "Zero created_at rate",
            format_ratio(comparison.left.local_summary.zero_created_at_rate),
            format_ratio(comparison.right.local_summary.zero_created_at_rate),
        ),
        (
            "Backfill rate",
            format_ratio(comparison.left.local_summary.backfill_rate),
            format_ratio(comparison.right.local_summary.backfill_rate),
        ),
        (
            "Raw p50 delta ms (biased)",
            format_latency_value(comparison.left.local_summary.raw_p50_delta_ms),
            format_latency_value(comparison.right.local_summary.raw_p50_delta_ms),
        ),
        (
            "Raw p95 delta ms (biased)",
            format_latency_value(comparison.left.local_summary.raw_p95_delta_ms),
            format_latency_value(comparison.right.local_summary.raw_p95_delta_ms),
        ),
        (
            "Raw p99 delta ms (biased)",
            format_latency_value(comparison.left.local_summary.raw_p99_delta_ms),
            format_latency_value(comparison.right.local_summary.raw_p99_delta_ms),
        ),
        (
            "Jitter p95-p50 ms",
            format_latency_value(comparison.left.local_summary.jitter_p95_minus_p50_ms),
            format_latency_value(comparison.right.local_summary.jitter_p95_minus_p50_ms),
        ),
        (
            "Jitter p99-p50 ms",
            format_latency_value(comparison.left.local_summary.jitter_p99_minus_p50_ms),
            format_latency_value(comparison.right.local_summary.jitter_p99_minus_p50_ms),
        ),
    ] {
        table.add_row(vec![label.to_string(), left_value, right_value]);
    }

    println!("{table}");
}

fn compute_yellowstone_created_at_summary(
    comparator: &Comparator,
    yellowstone_endpoint_names: &[String],
) -> Option<YellowstoneCreatedAtSummary> {
    if yellowstone_endpoint_names.is_empty() {
        return None;
    }

    let mut endpoint_stats: HashMap<String, YellowstoneCreatedAtEndpointStats> =
        HashMap::with_capacity(yellowstone_endpoint_names.len());
    for endpoint_name in yellowstone_endpoint_names {
        endpoint_stats.insert(
            endpoint_name.clone(),
            YellowstoneCreatedAtEndpointStats::default(),
        );
    }

    let mut eligible_signatures = 0usize;

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        let mut deltas = Vec::with_capacity(yellowstone_endpoint_names.len());
        let mut is_eligible = true;

        for endpoint_name in yellowstone_endpoint_names {
            let Some(tx) = sig_data.get(endpoint_name) else {
                is_eligible = false;
                break;
            };

            if tx.wallclock_secs < tx.start_wallclock_secs {
                is_eligible = false;
                break;
            }

            let Some(delta_ms) = tx.yellowstone_created_at_delta_ms else {
                is_eligible = false;
                break;
            };

            deltas.push((endpoint_name.clone(), delta_ms));
        }

        if !is_eligible {
            continue;
        }

        eligible_signatures += 1;
        for (endpoint_name, delta_ms) in deltas {
            if let Some(stats) = endpoint_stats.get_mut(&endpoint_name) {
                stats.deltas_ms.push(delta_ms);
            }
        }
    }

    let endpoints = endpoint_stats
        .into_iter()
        .map(|(endpoint, stats)| build_yellowstone_created_at_endpoint_summary(endpoint, stats))
        .collect();

    Some(YellowstoneCreatedAtSummary {
        eligible_signatures,
        endpoints,
        has_data: eligible_signatures > 0,
    })
}

fn compute_yellowstone_endpoint_local_summaries(
    comparator: &Comparator,
    endpoints: &[Endpoint],
) -> BTreeMap<String, YellowstoneEndpointLocalSummary> {
    let yellowstone_endpoints = endpoints
        .iter()
        .filter(|endpoint| endpoint.kind == EndpointKind::Yellowstone)
        .collect::<Vec<_>>();

    let mut stats: HashMap<String, YellowstoneEndpointLocalStats> = yellowstone_endpoints
        .iter()
        .map(|endpoint| {
            (
                endpoint.name.clone(),
                YellowstoneEndpointLocalStats::default(),
            )
        })
        .collect();

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();

        for endpoint in &yellowstone_endpoints {
            let Some(tx) = sig_data.get(&endpoint.name) else {
                continue;
            };

            let Some(endpoint_stats) = stats.get_mut(&endpoint.name) else {
                continue;
            };

            endpoint_stats.observed_signatures += 1;

            if tx.wallclock_secs < tx.start_wallclock_secs {
                endpoint_stats.backfill_transactions += 1;
                continue;
            }

            endpoint_stats.live_observations += 1;

            match (
                tx.yellowstone_created_at_delta_ms,
                tx.yellowstone_created_at_zero,
            ) {
                (Some(delta_ms), _) => {
                    endpoint_stats.eligible_created_at += 1;
                    endpoint_stats.deltas_ms.push(delta_ms);
                }
                (None, true) => {
                    endpoint_stats.zero_created_at += 1;
                }
                (None, false) => {
                    endpoint_stats.missing_created_at += 1;
                }
            }
        }
    }

    yellowstone_endpoints
        .into_iter()
        .map(|endpoint| {
            let summary = stats
                .remove(&endpoint.name)
                .map(build_yellowstone_endpoint_local_summary)
                .unwrap_or_default();
            (endpoint.name.clone(), summary)
        })
        .collect()
}

fn display_yellowstone_created_at_summary(summary: &YellowstoneCreatedAtSummary) {
    println!("\nYellowstone created_at delta");
    println!("--------------------------------------------");
    println!("Eligible signatures: {}", summary.eligible_signatures);

    if !summary.has_data {
        println!("Not enough data");
        return;
    }

    let mut table_rows: Vec<&YellowstoneCreatedAtEndpointSummary> =
        summary.endpoints.iter().collect();
    table_rows.sort_by(|lhs, rhs| compare_created_at_delta(lhs, rhs));

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["Endpoint", "Avg ms", "P50 ms", "P95 ms", "P99 ms"]);

    for summary in table_rows {
        table.add_row(vec![
            summary.name.clone(),
            format_latency_value(summary.avg_delta_ms),
            format_latency_value(summary.p50_delta_ms),
            format_latency_value(summary.p95_delta_ms),
            format_latency_value(summary.p99_delta_ms),
        ]);
    }

    println!("{table}");
}

fn select_comparable_endpoint(report: &MetricsReport) -> Result<MetricsComparisonSide> {
    let mut endpoints = report.per_endpoint.iter().filter_map(|(name, endpoint)| {
        endpoint
            .yellowstone_endpoint_local
            .as_ref()
            .map(|summary| MetricsComparisonSide {
                endpoint_name: name.clone(),
                endpoint_metrics: endpoint.clone(),
                local_summary: summary.clone(),
            })
    });

    let Some(first) = endpoints.next() else {
        bail!("metrics report does not contain a comparable Yellowstone endpoint");
    };

    if endpoints.next().is_some() {
        bail!("metrics report must contain exactly one Yellowstone endpoint for comparison");
    }

    Ok(first)
}

fn collect_reliability_issues(
    side: &str,
    summary: &YellowstoneEndpointLocalSummary,
    reasons: &mut Vec<String>,
) {
    let eligible_ratio = summary.eligible_ratio.unwrap_or(0.0);
    if eligible_ratio < 0.8 {
        reasons.push(format!(
            "{side} eligible_ratio {:.2}% < 80.00%",
            eligible_ratio * 100.0
        ));
    }
    if summary.eligible_created_at < 200 {
        reasons.push(format!(
            "{side} eligible_created_at {} < 200",
            summary.eligible_created_at
        ));
    }
}

fn diff_ms(tx: &TransactionData, first_tx: &TransactionData) -> f64 {
    let delta: Duration = tx
        .elapsed_since_start
        .saturating_sub(first_tx.elapsed_since_start);
    delta.as_secs_f64() * 1_000.0
}

fn build_summary(
    endpoint: String,
    stats: EndpointStats,
    total_signatures: usize,
) -> EndpointSummary {
    let mut summary = EndpointSummary {
        name: endpoint,
        valid_transactions: stats.total_observations,
        first_detections: stats.first_detections,
        backfill_transactions: stats.backfill_transactions,
        ..Default::default()
    };

    if total_signatures > 0 {
        summary.first_share = stats.first_detections as f64 / total_signatures as f64;
    }

    if !stats.delays_ms.is_empty() {
        let mut sorted = stats.delays_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        summary.p50_delay_ms = Some(percentile(&sorted, 0.5));
        summary.p95_delay_ms = Some(percentile(&sorted, 0.95));
        summary.p99_delay_ms = Some(percentile(&sorted, 0.99));
    }

    summary
}

fn build_yellowstone_created_at_endpoint_summary(
    endpoint: String,
    stats: YellowstoneCreatedAtEndpointStats,
) -> YellowstoneCreatedAtEndpointSummary {
    let mut summary = YellowstoneCreatedAtEndpointSummary {
        name: endpoint,
        ..Default::default()
    };

    if !stats.deltas_ms.is_empty() {
        let mut sorted = stats.deltas_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        summary.avg_delta_ms = Some(sorted.iter().sum::<f64>() / sorted.len() as f64);
        summary.p50_delta_ms = Some(percentile(&sorted, 0.5));
        summary.p95_delta_ms = Some(percentile(&sorted, 0.95));
        summary.p99_delta_ms = Some(percentile(&sorted, 0.99));
    }

    summary
}

fn build_yellowstone_endpoint_local_summary(
    mut stats: YellowstoneEndpointLocalStats,
) -> YellowstoneEndpointLocalSummary {
    stats
        .deltas_ms
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let raw_p50_delta_ms = percentile_if_any(&stats.deltas_ms, 0.5);
    let raw_p95_delta_ms = percentile_if_any(&stats.deltas_ms, 0.95);
    let raw_p99_delta_ms = percentile_if_any(&stats.deltas_ms, 0.99);

    YellowstoneEndpointLocalSummary {
        observed_signatures: stats.observed_signatures,
        live_observations: stats.live_observations,
        eligible_created_at: stats.eligible_created_at,
        missing_created_at: stats.missing_created_at,
        zero_created_at: stats.zero_created_at,
        eligible_ratio: ratio(stats.eligible_created_at, stats.live_observations),
        zero_created_at_rate: ratio(stats.zero_created_at, stats.live_observations),
        backfill_rate: ratio(stats.backfill_transactions, stats.observed_signatures),
        raw_p50_delta_ms,
        raw_p95_delta_ms,
        raw_p99_delta_ms,
        jitter_p95_minus_p50_ms: diff_option(raw_p95_delta_ms, raw_p50_delta_ms),
        jitter_p99_minus_p50_ms: diff_option(raw_p99_delta_ms, raw_p50_delta_ms),
    }
}

fn percentile_if_any(sorted: &[f64], percentile_value: f64) -> Option<f64> {
    (!sorted.is_empty()).then(|| percentile(sorted, percentile_value))
}

fn diff_option(lhs: Option<f64>, rhs: Option<f64>) -> Option<f64> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs - rhs),
        _ => None,
    }
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}

fn format_latency_value(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.2}"))
        .unwrap_or_else(|| "—".to_string())
}

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{:.2}", value * 100.0)
    } else {
        "—".to_string()
    }
}

fn format_ratio(value: Option<f64>) -> String {
    value
        .map(|ratio| format!("{:.2}%", ratio * 100.0))
        .unwrap_or_else(|| "—".to_string())
}

fn compare_latency(lhs: &EndpointSummary, rhs: &EndpointSummary) -> Ordering {
    match (lhs.p50_delay_ms, rhs.p50_delay_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}

fn compare_created_at_delta(
    lhs: &YellowstoneCreatedAtEndpointSummary,
    rhs: &YellowstoneCreatedAtEndpointSummary,
) -> Ordering {
    match (lhs.p50_delta_ms, rhs.p50_delta_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}

fn compare_quality(
    lhs: &YellowstoneEndpointLocalSummary,
    rhs: &YellowstoneEndpointLocalSummary,
) -> Ordering {
    compare_optional_desc(lhs.eligible_ratio, rhs.eligible_ratio)
        .then_with(|| compare_optional_asc(lhs.zero_created_at_rate, rhs.zero_created_at_rate))
        .then_with(|| compare_optional_asc(lhs.backfill_rate, rhs.backfill_rate))
}

fn compare_stability(
    lhs: &YellowstoneEndpointLocalSummary,
    rhs: &YellowstoneEndpointLocalSummary,
) -> Ordering {
    compare_optional_asc(lhs.jitter_p99_minus_p50_ms, rhs.jitter_p99_minus_p50_ms).then_with(|| {
        compare_optional_asc(lhs.jitter_p95_minus_p50_ms, rhs.jitter_p95_minus_p50_ms)
    })
}

fn compare_optional_asc(lhs: Option<f64>, rhs: Option<f64>) -> Ordering {
    match (lhs, rhs) {
        (Some(l), Some(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_optional_desc(lhs: Option<f64>, rhs: Option<f64>) -> Ordering {
    compare_optional_asc(rhs, lhs)
}

fn ordering_to_winner(ordering: Ordering) -> ComparisonWinner {
    match ordering {
        Ordering::Less => ComparisonWinner::Left,
        Ordering::Greater => ComparisonWinner::Right,
        Ordering::Equal => ComparisonWinner::Tie,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ComparisonWinner, MetricsReport, build_metrics_report, compare_metrics_reports,
        compute_run_summary,
    };
    use crate::{
        config::{ArgsCommitment, Config, Endpoint, EndpointKind},
        utils::Comparator,
        utils::TransactionData,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    fn tx(
        wallclock_secs: f64,
        start_wallclock_secs: f64,
        elapsed_ms: u64,
        yellowstone_created_at_delta_ms: Option<f64>,
    ) -> TransactionData {
        tx_with_zero(
            wallclock_secs,
            start_wallclock_secs,
            elapsed_ms,
            yellowstone_created_at_delta_ms,
            false,
        )
    }

    fn tx_with_zero(
        wallclock_secs: f64,
        start_wallclock_secs: f64,
        elapsed_ms: u64,
        yellowstone_created_at_delta_ms: Option<f64>,
        yellowstone_created_at_zero: bool,
    ) -> TransactionData {
        TransactionData {
            wallclock_secs,
            elapsed_since_start: Duration::from_millis(elapsed_ms),
            start_wallclock_secs,
            yellowstone_created_at_delta_ms,
            yellowstone_created_at_zero,
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn yellowstone_endpoint(name: &str, url: &str) -> Endpoint {
        Endpoint {
            name: name.to_string(),
            url: url.to_string(),
            x_token: None,
            kind: EndpointKind::Yellowstone,
        }
    }

    fn yellowstone_report(
        comparator: &Comparator,
        endpoint: Endpoint,
        run_started_at_unix_secs: f64,
        run_finished_at_unix_secs: f64,
    ) -> MetricsReport {
        let config = Config {
            transactions: 1_000,
            account: vec!["11111111111111111111111111111111".to_string()],
            commitment: ArgsCommitment::Processed,
        };
        let endpoints = vec![endpoint];
        let endpoint_names = endpoints
            .iter()
            .map(|value| value.name.clone())
            .collect::<Vec<_>>();
        let yellowstone_names = endpoint_names.clone();
        let summary = compute_run_summary(comparator, &endpoint_names, &yellowstone_names);

        build_metrics_report(
            &summary,
            comparator,
            &config,
            &endpoints,
            run_started_at_unix_secs,
            run_finished_at_unix_secs,
        )
    }

    #[test]
    fn yellowstone_created_at_uses_yellowstone_intersection_only() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 10, Some(5.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 20, Some(15.0))),
            ]),
        );
        comparator.add_batch(
            "ys-b",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 12, Some(7.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 18, Some(17.0))),
            ]),
        );
        comparator.add_batch(
            "arpc",
            HashMap::from([("sig-1".to_string(), tx(101.0, 100.0, 15, None))]),
        );

        let summary = compute_run_summary(
            &comparator,
            &names(&["ys-a", "ys-b", "arpc"]),
            &names(&["ys-a", "ys-b"]),
        );

        assert_eq!(summary.total_signatures, 1);

        let yellowstone = summary
            .yellowstone_created_at
            .expect("yellowstone summary should exist");
        assert_eq!(yellowstone.eligible_signatures, 2);

        let ys_a = yellowstone
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "ys-a")
            .expect("ys-a should be present");
        let ys_b = yellowstone
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "ys-b")
            .expect("ys-b should be present");

        assert_eq!(ys_a.avg_delta_ms, Some(10.0));
        assert_eq!(ys_b.avg_delta_ms, Some(12.0));
    }

    #[test]
    fn yellowstone_created_at_skips_missing_deltas() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([("sig-1".to_string(), tx(101.0, 100.0, 10, Some(5.0)))]),
        );
        comparator.add_batch(
            "ys-b",
            HashMap::from([("sig-1".to_string(), tx(101.0, 100.0, 11, None))]),
        );

        let summary = compute_run_summary(
            &comparator,
            &names(&["ys-a", "ys-b"]),
            &names(&["ys-a", "ys-b"]),
        );
        let yellowstone = summary
            .yellowstone_created_at
            .expect("yellowstone summary should exist");

        assert_eq!(yellowstone.eligible_signatures, 0);
        assert!(!yellowstone.has_data);
        assert!(
            yellowstone
                .endpoints
                .iter()
                .all(|endpoint| endpoint.avg_delta_ms.is_none())
        );
    }

    #[test]
    fn yellowstone_created_at_preserves_negative_values_and_percentiles() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 10, Some(-10.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 11, Some(20.0))),
                ("sig-3".to_string(), tx(103.0, 100.0, 12, Some(30.0))),
            ]),
        );

        let summary = compute_run_summary(&comparator, &names(&["ys-a"]), &names(&["ys-a"]));
        let yellowstone = summary
            .yellowstone_created_at
            .expect("yellowstone summary should exist");
        let endpoint = yellowstone
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "ys-a")
            .expect("ys-a should be present");

        assert_eq!(yellowstone.eligible_signatures, 3);
        assert!((endpoint.avg_delta_ms.expect("avg should exist") - 13.333_333_333).abs() < 1e-9);
        assert_eq!(endpoint.p50_delta_ms, Some(20.0));
        assert_eq!(endpoint.p95_delta_ms, Some(30.0));
        assert_eq!(endpoint.p99_delta_ms, Some(30.0));
    }

    #[test]
    fn yellowstone_created_at_skips_backfill_signatures() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-live".to_string(), tx(101.0, 100.0, 10, Some(5.0))),
                ("sig-backfill".to_string(), tx(99.0, 100.0, 20, Some(9.0))),
            ]),
        );
        comparator.add_batch(
            "ys-b",
            HashMap::from([
                ("sig-live".to_string(), tx(101.0, 100.0, 12, Some(6.0))),
                ("sig-backfill".to_string(), tx(99.0, 100.0, 18, Some(8.0))),
            ]),
        );

        let summary = compute_run_summary(
            &comparator,
            &names(&["ys-a", "ys-b"]),
            &names(&["ys-a", "ys-b"]),
        );
        let yellowstone = summary
            .yellowstone_created_at
            .expect("yellowstone summary should exist");

        assert_eq!(yellowstone.eligible_signatures, 1);
    }

    #[test]
    fn yellowstone_created_at_supports_single_endpoint() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 10, Some(5.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 12, Some(7.0))),
            ]),
        );

        let summary =
            compute_run_summary(&comparator, &names(&["ys-a", "arpc"]), &names(&["ys-a"]));
        let yellowstone = summary
            .yellowstone_created_at
            .expect("yellowstone summary should exist");
        let endpoint = yellowstone
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "ys-a")
            .expect("ys-a should be present");

        assert_eq!(yellowstone.eligible_signatures, 2);
        assert_eq!(endpoint.avg_delta_ms, Some(6.0));
        assert_eq!(endpoint.p50_delta_ms, Some(7.0));
    }

    #[test]
    fn zero_created_at_is_classified_but_not_counted_in_latency_distribution() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-valid".to_string(), tx(101.0, 100.0, 10, Some(5.0))),
                (
                    "sig-zero".to_string(),
                    tx_with_zero(102.0, 100.0, 11, None, true),
                ),
            ]),
        );

        let report = yellowstone_report(
            &comparator,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let local = report
            .per_endpoint
            .get("ys-a")
            .and_then(|endpoint| endpoint.yellowstone_endpoint_local.as_ref())
            .expect("local summary should exist");

        assert_eq!(
            report
                .yellowstone_created_at
                .as_ref()
                .map(|value| value.eligible_signatures),
            Some(1)
        );
        assert_eq!(local.eligible_created_at, 1);
        assert_eq!(local.zero_created_at, 1);
        assert_eq!(local.raw_p50_delta_ms, Some(5.0));
    }

    #[test]
    fn local_summary_tracks_missing_zero_valid_and_backfill() {
        let comparator = Comparator::new();

        comparator.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-valid".to_string(), tx(101.0, 100.0, 10, Some(5.0))),
                ("sig-missing".to_string(), tx(102.0, 100.0, 11, None)),
                (
                    "sig-zero".to_string(),
                    tx_with_zero(103.0, 100.0, 12, None, true),
                ),
                ("sig-backfill".to_string(), tx(99.0, 100.0, 13, Some(9.0))),
            ]),
        );

        let report = yellowstone_report(
            &comparator,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let local = report
            .per_endpoint
            .get("ys-a")
            .and_then(|endpoint| endpoint.yellowstone_endpoint_local.as_ref())
            .expect("local summary should exist");

        assert_eq!(local.observed_signatures, 4);
        assert_eq!(local.live_observations, 3);
        assert_eq!(local.eligible_created_at, 1);
        assert_eq!(local.missing_created_at, 1);
        assert_eq!(local.zero_created_at, 1);
        assert_eq!(local.eligible_ratio, Some(1.0 / 3.0));
        assert_eq!(local.zero_created_at_rate, Some(1.0 / 3.0));
        assert_eq!(local.backfill_rate, Some(0.25));
        assert_eq!(local.raw_p50_delta_ms, Some(5.0));
    }

    #[test]
    fn constant_offset_changes_raw_delta_but_not_jitter() {
        let base = Comparator::new();
        base.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 10, Some(10.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 11, Some(20.0))),
                ("sig-3".to_string(), tx(103.0, 100.0, 12, Some(30.0))),
                ("sig-4".to_string(), tx(104.0, 100.0, 13, Some(40.0))),
            ]),
        );
        let shifted = Comparator::new();
        shifted.add_batch(
            "ys-a",
            HashMap::from([
                ("sig-1".to_string(), tx(101.0, 100.0, 10, Some(110.0))),
                ("sig-2".to_string(), tx(102.0, 100.0, 11, Some(120.0))),
                ("sig-3".to_string(), tx(103.0, 100.0, 12, Some(130.0))),
                ("sig-4".to_string(), tx(104.0, 100.0, 13, Some(140.0))),
            ]),
        );

        let base_report = yellowstone_report(
            &base,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let shifted_report = yellowstone_report(
            &shifted,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );

        let base_local = base_report
            .per_endpoint
            .get("ys-a")
            .and_then(|endpoint| endpoint.yellowstone_endpoint_local.as_ref())
            .expect("base local summary should exist");
        let shifted_local = shifted_report
            .per_endpoint
            .get("ys-a")
            .and_then(|endpoint| endpoint.yellowstone_endpoint_local.as_ref())
            .expect("shifted local summary should exist");

        assert_ne!(base_local.raw_p50_delta_ms, shifted_local.raw_p50_delta_ms);
        assert_eq!(
            base_local.jitter_p95_minus_p50_ms,
            shifted_local.jitter_p95_minus_p50_ms
        );
        assert_eq!(
            base_local.jitter_p99_minus_p50_ms,
            shifted_local.jitter_p99_minus_p50_ms
        );
    }

    #[test]
    fn metrics_report_contains_compare_metadata() {
        let comparator = Comparator::new();
        comparator.add_batch(
            "ys-a",
            HashMap::from([("sig-1".to_string(), tx(101.0, 100.0, 10, Some(5.0)))]),
        );

        let report = yellowstone_report(
            &comparator,
            yellowstone_endpoint("ys-a", "https://example.com"),
            123.0,
            456.0,
        );
        let endpoint = report
            .per_endpoint
            .get("ys-a")
            .expect("endpoint should exist");

        assert_eq!(
            report.account,
            vec!["11111111111111111111111111111111".to_string()]
        );
        assert_eq!(report.commitment, "processed");
        assert_eq!(report.run_started_at_unix_ms, 123_000.0);
        assert_eq!(report.run_finished_at_unix_ms, 456_000.0);
        assert_eq!(endpoint.endpoint_url, "https://example.com");
        assert_eq!(endpoint.endpoint_kind, "yellowstone");
    }

    #[test]
    fn compare_rejects_metadata_mismatch() {
        let comparator = Comparator::new();
        comparator.add_batch(
            "ys-a",
            HashMap::from([("sig-1".to_string(), tx(101.0, 100.0, 10, Some(5.0)))]),
        );

        let left = yellowstone_report(
            &comparator,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let right = yellowstone_report(
            &comparator,
            yellowstone_endpoint("ys-a", "https://other.example.com"),
            100.0,
            110.0,
        );

        let err = compare_metrics_reports(&left, &right).expect_err("comparison should fail");
        assert!(err.to_string().contains("endpoint_url"));
    }

    #[test]
    fn compare_marks_low_coverage_as_unreliable() {
        let left = Comparator::new();
        let right = Comparator::new();

        let mut left_entries = HashMap::new();
        let mut right_entries = HashMap::new();
        for index in 0..220 {
            let signature = format!("sig-{index}");
            left_entries.insert(
                signature.clone(),
                tx(101.0 + index as f64, 100.0, 10, Some(10.0)),
            );
            let value = if index < 150 {
                tx(101.0 + index as f64, 100.0, 10, Some(20.0))
            } else {
                tx_with_zero(101.0 + index as f64, 100.0, 10, None, true)
            };
            right_entries.insert(signature, value);
        }

        left.add_batch("ys-a", left_entries);
        right.add_batch("ys-a", right_entries);

        let left_report = yellowstone_report(
            &left,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let right_report = yellowstone_report(
            &right,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );

        let comparison = compare_metrics_reports(&left_report, &right_report)
            .expect("comparison should succeed");

        assert!(!comparison.reliable_for_latency);
        assert!(comparison.stability_winner.is_none());
        assert!(
            comparison
                .unreliability_reasons
                .iter()
                .any(|reason| reason.contains("eligible_ratio"))
        );
    }

    #[test]
    fn compare_returns_stability_and_quality_winners_for_reliable_reports() {
        let left = Comparator::new();
        let right = Comparator::new();

        let mut left_entries = HashMap::new();
        let mut right_entries = HashMap::new();
        for index in 0..240 {
            let signature = format!("sig-{index}");
            let left_delta = 100.0 + (index % 3) as f64;
            let right_value = if index < 210 {
                let spread = (index % 40) as f64;
                tx(101.0 + index as f64, 100.0, 10, Some(100.0 + spread))
            } else if index < 225 {
                tx_with_zero(101.0 + index as f64, 100.0, 10, None, true)
            } else {
                tx(99.0, 100.0, 10, Some(150.0))
            };

            left_entries.insert(
                signature.clone(),
                tx(101.0 + index as f64, 100.0, 10, Some(left_delta)),
            );
            right_entries.insert(signature, right_value);
        }

        left.add_batch("ys-a", left_entries);
        right.add_batch("ys-a", right_entries);

        let left_report = yellowstone_report(
            &left,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );
        let right_report = yellowstone_report(
            &right,
            yellowstone_endpoint("ys-a", "https://example.com"),
            100.0,
            110.0,
        );

        let comparison = compare_metrics_reports(&left_report, &right_report)
            .expect("comparison should succeed");

        assert!(comparison.reliable_for_latency);
        assert_eq!(comparison.stability_winner, Some(ComparisonWinner::Left));
        assert_eq!(comparison.data_quality_winner, ComparisonWinner::Left);
    }
}
