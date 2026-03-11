use crate::utils::{Comparator, TransactionData, percentile};
use comfy_table::{ContentArrangement, Table};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

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

#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub yellowstone_created_at: Option<YellowstoneCreatedAtSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
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
                    .map(|v| format!("{:.2}ms", v))
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

        println!("{}", table);
    }

    if let Some(yellowstone_summary) = summary.yellowstone_created_at.as_ref() {
        display_yellowstone_created_at_summary(yellowstone_summary);
    }
}

pub fn build_metrics_report(summary: &RunSummary) -> Value {
    let mut per_endpoint = Map::new();
    for endpoint in &summary.endpoints {
        let payload = json!({
            "first_detection_rate": endpoint.first_share,
            "p50_latency_ms": endpoint.p50_delay_ms,
            "p95_latency_ms": endpoint.p95_delay_ms,
            "p99_latency_ms": endpoint.p99_delay_ms,
            "observations": endpoint.valid_transactions,
            "first_detections": endpoint.first_detections,
            "backfill_transactions": endpoint.backfill_transactions,
        });
        per_endpoint.insert(endpoint.name.clone(), payload);
    }

    let yellowstone_created_at = summary.yellowstone_created_at.as_ref().map(|payload| {
        let mut per_endpoint = Map::new();
        for endpoint in &payload.endpoints {
            per_endpoint.insert(
                endpoint.name.clone(),
                json!({
                    "avg_delta_ms": endpoint.avg_delta_ms,
                    "p50_delta_ms": endpoint.p50_delta_ms,
                    "p95_delta_ms": endpoint.p95_delta_ms,
                    "p99_delta_ms": endpoint.p99_delta_ms,
                }),
            );
        }

        json!({
            "eligible_signatures": payload.eligible_signatures,
            "per_endpoint": per_endpoint,
        })
    });

    json!({
        "total_signatures": summary.total_signatures,
        "backfill_signatures": summary.backfill_signatures,
        "per_endpoint": per_endpoint,
        "yellowstone_created_at": yellowstone_created_at,
    })
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
    table_rows.sort_by(|a, b| compare_created_at_delta(a, b));

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

    println!("{}", table);
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

fn format_latency_value(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
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

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{:.2}", value * 100.0)
    } else {
        "—".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::compute_run_summary;
    use crate::utils::Comparator;
    use crate::utils::TransactionData;
    use std::collections::HashMap;
    use std::time::Duration;

    fn tx(
        wallclock_secs: f64,
        start_wallclock_secs: f64,
        elapsed_ms: u64,
        yellowstone_created_at_delta_ms: Option<f64>,
    ) -> TransactionData {
        TransactionData {
            wallclock_secs,
            elapsed_since_start: Duration::from_millis(elapsed_ms),
            start_wallclock_secs,
            yellowstone_created_at_delta_ms,
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
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
}
