use std::{error::Error, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_transaction::versioned::VersionedTransaction;
use tokio::{task, time::MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{Level, info, warn};
use url::Url;

use crate::{
    config::{Config, Endpoint, HELIUS_PRECONF_HOST, HeliusPreconfRegion},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{GeyserProvider, ProviderContext};

const SUBSCRIBE_REQUEST_ID: u64 = 1;
const BINARY_HEADER_LEN: usize = 18;
const BINARY_SCHEMA_VERSION: u8 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct HeliusPreconfProvider;

impl GeyserProvider for HeliusPreconfProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        let shutdown_tx = context.shutdown_tx.clone();
        task::spawn(async move {
            let result = process_helius_preconf_endpoint(endpoint, config, context).await;
            if result.is_err() {
                let _ = shutdown_tx.send(());
            }
            result.map_err(Into::into)
        })
    }
}

async fn process_helius_preconf_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<()> {
    let ProviderContext {
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        ..
    } = context;
    let endpoint_url = build_endpoint_url(&endpoint.url, endpoint.x_token.as_deref())?;
    let endpoint_origin = endpoint_url.origin().ascii_serialization();
    let subscribe_request = build_subscribe_request(&config.account, &endpoint.region_include);
    let endpoint_name = endpoint.name;
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    info!(endpoint = %endpoint_name, origin = %endpoint_origin, "Connecting");
    let connect_result = tokio::select! { biased;
        _ = shutdown_rx.recv() => {
            info!(endpoint = %endpoint_name, "Received stop signal while connecting");
            return Ok(());
        }
        result = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(endpoint_url.as_str())) => {
            result.context("timed out connecting to Helius preconf endpoint")?
        },
    };
    let (mut websocket, _) = connect_result.with_context(|| {
        format!("failed to connect to Helius preconf endpoint {endpoint_origin}")
    })?;

    tokio::select! { biased;
        _ = shutdown_rx.recv() => {
            info!(endpoint = %endpoint_name, "Received stop signal before subscribing");
            close_websocket(&endpoint_name, &mut websocket).await;
            return Ok(());
        }
        result = tokio::time::timeout(
            WRITE_TIMEOUT,
            websocket.send(Message::Text(subscribe_request)),
        ) => {
            result
                .context("timed out sending Helius preconfSubscribe request")?
                .context("failed to send Helius preconfSubscribe request")?;
        }
    }

    let mut ping_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let subscribe_ack_deadline = tokio::time::Instant::now() + SUBSCRIBE_ACK_TIMEOUT;
    let subscription_id = loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                close_websocket(&endpoint_name, &mut websocket).await;
                return Ok(());
            }
            _ = tokio::time::sleep_until(subscribe_ack_deadline) => {
                close_websocket(&endpoint_name, &mut websocket).await;
                bail!("timed out waiting for Helius preconfSubscribe acknowledgement");
            }
            _ = ping_interval.tick() => {
                send_ping(&mut websocket).await?;
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    bail!("Helius preconf websocket closed before subscribe acknowledgement");
                };
                match message.context("Helius preconf websocket error while awaiting subscribe acknowledgement")? {
                    Message::Text(text) => break parse_subscribe_ack(&text, SUBSCRIBE_REQUEST_ID)?,
                    Message::Binary(payload) => warn!(
                        endpoint = %endpoint_name,
                        frame_len = payload.len(),
                        "Received binary notification before subscribe acknowledgement"
                    ),
                    Message::Close(frame) => bail!(
                        "Helius preconf websocket closed before subscribe acknowledgement: {frame:?}"
                    ),
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Frame(_) => warn!(
                        endpoint = %endpoint_name,
                        "Received unexpected raw websocket frame before subscribe acknowledgement"
                    ),
                }
            }
        }
    };

    comparator.mark_ready();
    info!(endpoint = %endpoint_name, subscription_id, "Connected");
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                close_websocket(&endpoint_name, &mut websocket).await;
                break;
            }
            _ = ping_interval.tick() => {
                send_ping(&mut websocket).await?;
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    bail!("Helius preconf websocket closed by server");
                };
                match message.context("Helius preconf websocket stream error")? {
                    Message::Binary(payload) => {
                        let wallclock_secs = get_current_timestamp();
                        let elapsed_since_start = start_instant.elapsed();
                        ensure_supported_schema_version(&payload)?;
                        let notification = match parse_binary_notification(&payload) {
                            Ok(notification) => notification,
                            Err(err) => {
                                warn!(
                                    endpoint = %endpoint_name,
                                    frame_len = payload.len(),
                                    error = %err,
                                    "Discarding invalid Helius preconf binary frame"
                                );
                                continue;
                            }
                        };

                        if let Some(file) = log_file.as_mut() {
                            write_log_entry(
                                file,
                                wallclock_secs,
                                &endpoint_name,
                                &notification.signature,
                            )?;
                        }

                        let tx_data = TransactionData {
                            wallclock_secs,
                            elapsed_since_start,
                            start_wallclock_secs,
                            yellowstone_created_at_delta_ms: None,
                            yellowstone_created_at_zero: false,
                        };
                        comparator.record_observation(
                            &endpoint_name,
                            &notification.signature,
                            tx_data,
                            1,
                        );
                        transaction_count += 1;
                    }
                    Message::Text(text) => {
                        validate_runtime_text(&text)?;
                        warn!(endpoint = %endpoint_name, "Ignoring unexpected JSON-RPC text response");
                    }
                    Message::Close(frame) => {
                        bail!("Helius preconf websocket closed by server: {frame:?}");
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Frame(_) => warn!(
                        endpoint = %endpoint_name,
                        "Received unexpected raw websocket frame"
                    ),
                }
            }
        }
    }

    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        "Stream closed after dispatching preconfirmations"
    );
    Ok(())
}

async fn send_ping(
    websocket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<()> {
    tokio::time::timeout(WRITE_TIMEOUT, websocket.send(Message::Ping(Vec::new())))
        .await
        .context("timed out sending Helius preconf websocket ping")?
        .context("failed to send Helius preconf websocket ping")
}

async fn close_websocket(
    endpoint_name: &str,
    websocket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    match tokio::time::timeout(CLOSE_TIMEOUT, websocket.close(None)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(
            endpoint = %endpoint_name,
            error = %err,
            "Failed to close Helius preconf websocket during shutdown"
        ),
        Err(_) => warn!(
            endpoint = %endpoint_name,
            "Timed out closing Helius preconf websocket during shutdown"
        ),
    }
}

fn build_endpoint_url(endpoint_url: &str, x_token: Option<&str>) -> Result<Url> {
    let mut url =
        Url::parse(endpoint_url).context("failed to parse Helius preconf endpoint URL")?;
    ensure!(
        url.scheme() == "wss",
        "Helius preconf endpoint URL must use wss"
    );
    ensure!(
        url.host_str() == Some(HELIUS_PRECONF_HOST) && url.port().is_none_or(|port| port == 443),
        "Helius preconf endpoint URL must use the official wss://{HELIUS_PRECONF_HOST}/ endpoint"
    );

    let has_api_key = url
        .query_pairs()
        .any(|(key, value)| key == "api-key" && !value.trim().is_empty());
    if !has_api_key {
        let api_key = x_token
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .context("Helius preconf endpoint requires api-key query parameter or x_token")?;
        let retained_query = url
            .query_pairs()
            .filter(|(key, _)| key != "api-key")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(retained_query)
            .append_pair("api-key", api_key);
    }

    Ok(url)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreconfSubscribeFilter<'a> {
    failed: bool,
    #[serde(skip_serializing_if = "<[HeliusPreconfRegion]>::is_empty")]
    region_include: &'a [HeliusPreconfRegion],
    account_include: &'a [String],
}

fn build_subscribe_request(accounts: &[String], region_include: &[HeliusPreconfRegion]) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": SUBSCRIBE_REQUEST_ID,
        "method": "preconfSubscribe",
        "params": [PreconfSubscribeFilter {
            failed: false,
            region_include,
            account_include: accounts,
        }],
    })
    .to_string()
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    jsonrpc: Option<String>,
    id: Option<Value>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

fn parse_json_rpc_response(text: &str) -> Result<JsonRpcResponse> {
    serde_json::from_str(text).context("failed to decode Helius JSON-RPC text frame")
}

fn ensure_no_json_rpc_error(response: &JsonRpcResponse) -> Result<()> {
    if let Some(error) = &response.error {
        bail!("Helius JSON-RPC error {}: {}", error.code, error.message);
    }
    Ok(())
}

fn parse_subscribe_ack(text: &str, expected_id: u64) -> Result<u64> {
    let response = parse_json_rpc_response(text)?;
    ensure_no_json_rpc_error(&response)?;
    ensure!(
        response.jsonrpc.as_deref() == Some("2.0"),
        "invalid Helius preconfSubscribe acknowledgement JSON-RPC version"
    );
    ensure!(
        response.id.as_ref().and_then(Value::as_u64) == Some(expected_id),
        "invalid Helius preconfSubscribe acknowledgement request id"
    );
    response
        .result
        .as_ref()
        .and_then(Value::as_u64)
        .context("invalid Helius preconfSubscribe acknowledgement subscription id")
}

fn validate_runtime_text(text: &str) -> Result<()> {
    let response = parse_json_rpc_response(text)?;
    ensure_no_json_rpc_error(&response)
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedPreconfNotification {
    slot: u64,
    tx_index: u64,
    status: u8,
    signature: String,
}

fn ensure_supported_schema_version(payload: &[u8]) -> Result<()> {
    if let Some(version) = payload.first() {
        ensure!(
            *version == BINARY_SCHEMA_VERSION,
            "unsupported Helius preconf binary schema version {version}"
        );
    }
    Ok(())
}

fn parse_binary_notification(payload: &[u8]) -> Result<ParsedPreconfNotification> {
    ensure!(
        payload.len() >= BINARY_HEADER_LEN,
        "binary notification is shorter than its {BINARY_HEADER_LEN}-byte header"
    );
    ensure!(
        payload[0] == BINARY_SCHEMA_VERSION,
        "unsupported Helius preconf binary schema version {}",
        payload[0]
    );

    let slot = u64::from_le_bytes(
        payload[1..9]
            .try_into()
            .context("invalid slot field in binary notification header")?,
    );
    let tx_index = u64::from_le_bytes(
        payload[9..17]
            .try_into()
            .context("invalid transaction index field in binary notification header")?,
    );
    let status = payload[17];
    ensure!(
        matches!(status, 0..=2),
        "invalid transaction status {status} in binary notification header"
    );

    let transaction =
        bincode::deserialize::<VersionedTransaction>(&payload[BINARY_HEADER_LEN..])
            .context("failed to deserialize VersionedTransaction from binary notification")?;
    let signature = transaction
        .signatures
        .first()
        .context("binary notification transaction has no signature")?
        .to_string();

    Ok(ParsedPreconfNotification {
        slot,
        tx_index,
        status,
        signature,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use solana_transaction::versioned::VersionedTransaction;

    use super::{
        BINARY_HEADER_LEN, build_endpoint_url, build_subscribe_request,
        ensure_supported_schema_version, parse_binary_notification, parse_subscribe_ack,
        validate_runtime_text,
    };
    use crate::config::HeliusPreconfRegion;

    #[test]
    fn builds_filtered_preconf_subscribe_request() {
        let accounts = vec![
            "11111111111111111111111111111111".to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ];

        let request: Value = serde_json::from_str(&build_subscribe_request(
            &accounts,
            &[HeliusPreconfRegion::Sgp, HeliusPreconfRegion::Tyo],
        ))
        .unwrap();

        assert_eq!(request["jsonrpc"], "2.0");
        assert_eq!(request["id"], 1);
        assert_eq!(request["method"], "preconfSubscribe");
        assert_eq!(request["params"][0]["failed"], false);
        assert_eq!(
            request["params"][0]["regionInclude"],
            serde_json::json!(["sgp", "tyo"])
        );
        assert_eq!(
            request["params"][0]["accountInclude"],
            serde_json::json!(accounts)
        );

        let unfiltered: Value =
            serde_json::from_str(&build_subscribe_request(&accounts, &[])).unwrap();
        assert!(unfiltered["params"][0].get("regionInclude").is_none());
    }

    #[test]
    fn validates_subscribe_acknowledgement_and_errors() {
        let subscription_id =
            parse_subscribe_ack(r#"{"jsonrpc":"2.0","result":24040,"id":1}"#, 1).unwrap();
        assert_eq!(subscription_id, 24_040);
        assert!(parse_subscribe_ack(r#"{"jsonrpc":"2.0","result":24040,"id":2}"#, 1).is_err());

        let error =
            r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"invalid params"},"id":1}"#;
        assert!(parse_subscribe_ack(error, 1).is_err());
        assert!(validate_runtime_text(error).is_err());
    }

    #[test]
    fn endpoint_url_uses_existing_key_or_appends_x_token() {
        let existing = build_endpoint_url(
            "wss://beta.helius-rpc.com/?api-key=url-key&other=value",
            Some("token-key"),
        )
        .unwrap();
        assert_eq!(
            existing
                .query_pairs()
                .filter(|(key, _)| key == "api-key")
                .map(|(_, value)| value.into_owned())
                .collect::<Vec<_>>(),
            vec!["url-key"]
        );

        let appended =
            build_endpoint_url("wss://beta.helius-rpc.com/?other=value", Some("token key"))
                .unwrap();
        assert_eq!(
            appended
                .query_pairs()
                .find(|(key, _)| key == "api-key")
                .map(|(_, value)| value.into_owned()),
            Some("token key".to_string())
        );
        assert_eq!(
            appended.origin().ascii_serialization(),
            "wss://beta.helius-rpc.com"
        );

        assert!(build_endpoint_url("ws://beta.helius-rpc.com", Some("key")).is_err());
        assert!(build_endpoint_url("wss://beta.helius-rpc.com", None).is_err());
        assert!(build_endpoint_url("wss://example.com", Some("key")).is_err());
    }

    #[test]
    fn parses_binary_notification_header_and_transaction() {
        let expected_signature = bs58::encode([7_u8; 64]).into_string();
        let mut transaction = VersionedTransaction::default();
        transaction
            .signatures
            .push(expected_signature.parse().unwrap());
        let mut payload = Vec::with_capacity(BINARY_HEADER_LEN + 256);
        payload.push(1);
        payload.extend_from_slice(&123_u64.to_le_bytes());
        payload.extend_from_slice(&45_u64.to_le_bytes());
        payload.push(2);
        payload.extend_from_slice(&bincode::serialize(&transaction).unwrap());

        let parsed = parse_binary_notification(&payload).unwrap();

        assert_eq!(parsed.slot, 123);
        assert_eq!(parsed.tx_index, 45);
        assert_eq!(parsed.status, 2);
        assert_eq!(parsed.signature, expected_signature);
    }

    #[test]
    fn rejects_unknown_schema_and_bad_binary_frames() {
        assert!(ensure_supported_schema_version(&[]).is_ok());
        assert!(ensure_supported_schema_version(&[1]).is_ok());
        assert!(ensure_supported_schema_version(&[2]).is_err());
        assert!(parse_binary_notification(&[1; BINARY_HEADER_LEN - 1]).is_err());

        let mut invalid_status = vec![0; BINARY_HEADER_LEN];
        invalid_status[0] = 1;
        invalid_status[17] = 3;
        assert!(parse_binary_notification(&invalid_status).is_err());

        let mut invalid_transaction = vec![0; BINARY_HEADER_LEN];
        invalid_transaction[0] = 1;
        invalid_transaction[17] = 1;
        assert!(parse_binary_notification(&invalid_transaction).is_err());
    }
}
