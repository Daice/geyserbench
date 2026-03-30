use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{Level, error, info, warn};

use crate::proto::geyser::{
    CommitmentLevel, SubscribeDeshredRequest, SubscribeRequest,
    SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterTransactions,
    SubscribeRequestPing, SubscribeUpdateDeshredTransactionInfo, subscribe_update::UpdateOneof,
    subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof,
};

use crate::{
    config::{Config, Endpoint, YellowstoneEndpointUrl, parse_yellowstone_endpoint_url},
    utils::{
        TransactionData, get_current_timestamp, open_log_file, protobuf_timestamp_to_unix_ms,
        write_log_entry,
    },
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, WatchedAccounts, build_signature_envelope, enqueue_signature,
        fatal_connection_error,
    },
    yellowstone_client::GeyserGrpcClient,
};

pub struct YellowstoneProvider;
pub struct YellowstoneDeshredProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_yellowstone_endpoint(endpoint, config, context).await })
    }
}

impl GeyserProvider for YellowstoneDeshredProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(
            async move { process_yellowstone_deshred_endpoint(endpoint, config, context).await },
        )
    }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;

    let watched_accounts = WatchedAccounts::new(&config.account)?;
    let endpoint_name = endpoint.name.clone();
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint
        .x_token
        .as_deref()
        .filter(|token| !token.trim().is_empty());

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client =
        connect_yellowstone_client(&endpoint_name, &endpoint_url, endpoint_token).await;

    info!(endpoint = %endpoint_name, "Connected");

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: CommitmentLevel = config.commitment.into();

    subscribe_tx
        .send(build_subscribe_request(
            watched_accounts.filters(),
            commitment,
        ))
        .await?;

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        let receive_wallclock_secs = get_current_timestamp();
                        let (created_at_delta_ms, created_at_zero) =
                            extract_created_at_observation(receive_wallclock_secs, msg.created_at.as_ref());

                        match msg.update_oneof {
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction.as_ref()
                                    && let Some(msg) = tx.transaction.as_ref().and_then(|t| t.message.as_ref()) {
                                        let has_account = transaction_has_matching_account_keys(
                                            &msg.account_keys,
                                            &watched_accounts,
                                        );

                                        if has_account {
                                            let wallclock = get_current_timestamp();
                                            let elapsed = start_instant.elapsed();
                                            let signature = match tx.transaction.as_ref()
                                                .and_then(|t| t.signatures.first()) {
                                                Some(sig) => bs58::encode(sig).into_string(),
                                                None => {
                                                    warn!(endpoint = %endpoint_name, "Missing signature in transaction");
                                                    continue;
                                                }
                                            };

                                            if let Some(file) = log_file.as_mut() {
                                                write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                                            }

                                            let tx_data = TransactionData {
                                                wallclock_secs: wallclock,
                                                elapsed_since_start: elapsed,
                                                start_wallclock_secs,
                                                yellowstone_created_at_delta_ms: created_at_delta_ms,
                                                yellowstone_created_at_zero: created_at_zero,
                                            };

                                            let updated = accumulator.record(
                                                signature.clone(),
                                                tx_data.clone(),
                                            );

                                            if updated
                                                && let Some(envelope) = build_signature_envelope(
                                                    &comparator,
                                                    &endpoint_name,
                                                    &signature,
                                                    tx_data,
                                                    total_producers,
                                                ) {
                                                    if let Some(target) = target_transactions {
                                                        let shared = shared_counter
                                                            .fetch_add(1, Ordering::AcqRel)
                                                            + 1;
                                                        if let Some(tracker) = progress.as_ref() {
                                                            tracker.record(shared);
                                                        }
                                                        if shared >= target
                                                            && !shared_shutdown.swap(true, Ordering::AcqRel)
                                                        {
                                                            info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                                                            let _ = shutdown_tx.send(());
                                                        }
                                                    }

                                                    if let Some(sender) = signature_sender.as_ref() {
                                                        enqueue_signature(sender, &endpoint_name, &signature, envelope);
                                                    }
                                                }

                                            transaction_count += 1;
                                        }
                                    }
                            },
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(build_ping_request(1))
                                    .await?;
                            },
                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Error receiving message from stream");
                        break;
                    },
                    None => {
                        info!(endpoint = %endpoint_name, "Stream closed by server");
                        break;
                    }
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}

async fn process_yellowstone_deshred_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;

    let watched_accounts = WatchedAccounts::new(&config.account)?;
    let endpoint_name = endpoint.name.clone();
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint
        .x_token
        .as_deref()
        .filter(|token| !token.trim().is_empty());

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client =
        connect_yellowstone_client(&endpoint_name, &endpoint_url, endpoint_token).await;

    info!(endpoint = %endpoint_name, "Connected");

    let (mut subscribe_tx, mut stream) = client.subscribe_deshred().await?;
    subscribe_tx
        .send(build_deshred_subscribe_request(watched_accounts.filters()))
        .await?;

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        let receive_wallclock_secs = get_current_timestamp();
                        let (created_at_delta_ms, created_at_zero) =
                            extract_created_at_observation(receive_wallclock_secs, msg.created_at.as_ref());

                        match msg.update_oneof {
                            Some(DeshredUpdateOneof::DeshredTransaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction.as_ref() {
                                    if !deshred_matches_watched_accounts(tx, &watched_accounts) {
                                        continue;
                                    }

                                    let wallclock = get_current_timestamp();
                                    let elapsed = start_instant.elapsed();
                                    let Some(signature) = deshred_signature_from_update(
                                        &DeshredUpdateOneof::DeshredTransaction(tx_msg),
                                    ) else {
                                        warn!(endpoint = %endpoint_name, "Missing signature in deshred transaction");
                                        continue;
                                    };

                                    if let Some(file) = log_file.as_mut() {
                                        write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                                    }

                                    let tx_data = TransactionData {
                                        wallclock_secs: wallclock,
                                        elapsed_since_start: elapsed,
                                        start_wallclock_secs,
                                        yellowstone_created_at_delta_ms: created_at_delta_ms,
                                        yellowstone_created_at_zero: created_at_zero,
                                    };

                                    let updated = accumulator.record(
                                        signature.clone(),
                                        tx_data.clone(),
                                    );

                                    if updated
                                        && let Some(envelope) = build_signature_envelope(
                                            &comparator,
                                            &endpoint_name,
                                            &signature,
                                            tx_data,
                                            total_producers,
                                        ) {
                                            if let Some(target) = target_transactions {
                                                let shared = shared_counter
                                                    .fetch_add(1, Ordering::AcqRel)
                                                    + 1;
                                                if let Some(tracker) = progress.as_ref() {
                                                    tracker.record(shared);
                                                }
                                                if shared >= target
                                                    && !shared_shutdown.swap(true, Ordering::AcqRel)
                                                {
                                                    info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                                                    let _ = shutdown_tx.send(());
                                                }
                                            }

                                            if let Some(sender) = signature_sender.as_ref() {
                                                enqueue_signature(sender, &endpoint_name, &signature, envelope);
                                            }
                                        }

                                    transaction_count += 1;
                                }
                            }
                            Some(DeshredUpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(build_deshred_ping_request(1))
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Error receiving message from deshred stream");
                        break;
                    }
                    None => {
                        info!(endpoint = %endpoint_name, "Deshred stream closed by server");
                        break;
                    }
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Deshred stream closed after dispatching transactions"
    );
    Ok(())
}

async fn connect_yellowstone_client(
    endpoint_name: &str,
    endpoint_url: &str,
    endpoint_token: Option<&str>,
) -> GeyserGrpcClient {
    let endpoint_transport = parse_yellowstone_endpoint_url(endpoint_url)
        .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err));

    let builder = match &endpoint_transport {
        YellowstoneEndpointUrl::Http | YellowstoneEndpointUrl::Https => {
            GeyserGrpcClient::build_from_shared(endpoint_url.to_owned())
                .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err))
        }
        YellowstoneEndpointUrl::Unix(_) => GeyserGrpcClient::build_from_static("http://[::]:0"),
    };
    let builder = if let Some(token) = endpoint_token {
        builder
            .x_token(Some(token))
            .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err))
    } else {
        builder
    };
    let builder = if matches!(endpoint_transport, YellowstoneEndpointUrl::Https) {
        builder
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err))
    } else {
        builder
    };

    match endpoint_transport {
        YellowstoneEndpointUrl::Http | YellowstoneEndpointUrl::Https => builder
            .connect()
            .await
            .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err)),
        YellowstoneEndpointUrl::Unix(path) => builder
            .connect_uds(path)
            .await
            .unwrap_or_else(|err| fatal_connection_error(endpoint_name, err)),
    }
}

fn build_subscribe_request(
    account_filters: &[String],
    commitment: CommitmentLevel,
) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "account".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: account_filters.to_vec(),
            account_exclude: vec![],
            account_required: vec![],
            ..Default::default()
        },
    );

    SubscribeRequest {
        slots: HashMap::default(),
        accounts: HashMap::default(),
        transactions,
        transactions_status: HashMap::default(),
        entry: HashMap::default(),
        blocks: HashMap::default(),
        blocks_meta: HashMap::default(),
        commitment: Some(commitment as i32),
        accounts_data_slice: Vec::default(),
        ping: None,
        from_slot: None,
    }
}

fn build_ping_request(id: i32) -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

fn build_deshred_subscribe_request(account_filters: &[String]) -> SubscribeDeshredRequest {
    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
        "account".to_string(),
        SubscribeRequestFilterDeshredTransactions {
            vote: None,
            account_include: account_filters.to_vec(),
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeDeshredRequest {
        deshred_transactions,
        ping: None,
    }
}

fn build_deshred_ping_request(id: i32) -> SubscribeDeshredRequest {
    SubscribeDeshredRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

fn extract_created_at_observation(
    receive_wallclock_secs: f64,
    created_at: Option<&prost_types::Timestamp>,
) -> (Option<f64>, bool) {
    let created_at_raw_ms = created_at.and_then(protobuf_timestamp_to_unix_ms);
    match created_at_raw_ms {
        Some(0.0) => (None, true),
        Some(created_at_ms) => (
            Some((receive_wallclock_secs * 1_000.0) - created_at_ms),
            false,
        ),
        None => (None, false),
    }
}

fn transaction_has_matching_account_keys(
    account_keys: &[Vec<u8>],
    watched_accounts: &WatchedAccounts,
) -> bool {
    account_keys
        .iter()
        .any(|key| watched_accounts.matches_bytes(key.as_slice()))
}

fn deshred_matches_watched_accounts(
    tx: &SubscribeUpdateDeshredTransactionInfo,
    watched_accounts: &WatchedAccounts,
) -> bool {
    let static_match = tx
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.message.as_ref())
        .map(|message| {
            transaction_has_matching_account_keys(&message.account_keys, watched_accounts)
        })
        .unwrap_or(false);

    static_match
        || tx
            .loaded_writable_addresses
            .iter()
            .any(|key| watched_accounts.matches_bytes(key.as_slice()))
        || tx
            .loaded_readonly_addresses
            .iter()
            .any(|key| watched_accounts.matches_bytes(key.as_slice()))
}

fn deshred_signature_from_update(update: &DeshredUpdateOneof) -> Option<String> {
    match update {
        DeshredUpdateOneof::DeshredTransaction(tx_msg) => tx_msg
            .transaction
            .as_ref()
            .map(|tx_info| bs58::encode(&tx_info.signature).into_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_deshred_ping_request, build_deshred_subscribe_request,
        deshred_matches_watched_accounts, deshred_signature_from_update,
    };
    use crate::proto::{
        geyser::{
            SubscribeUpdateDeshred, SubscribeUpdateDeshredTransaction,
            SubscribeUpdateDeshredTransactionInfo,
            subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof,
        },
        solana::storage::confirmed_block::{Message, Transaction},
    };
    use crate::providers::common::WatchedAccounts;

    #[test]
    fn build_deshred_subscribe_request_uses_account_filters() {
        let request = build_deshred_subscribe_request(&[String::from("account-1")]);
        let filter = request
            .deshred_transactions
            .get("account")
            .expect("account filter should exist");

        assert_eq!(filter.account_include, vec![String::from("account-1")]);
        assert!(filter.account_exclude.is_empty());
        assert!(filter.account_required.is_empty());
        assert!(request.ping.is_none());
    }

    #[test]
    fn deshred_signature_from_update_returns_base58_string() {
        let update = SubscribeUpdateDeshred {
            filters: Vec::new(),
            created_at: None,
            update_oneof: Some(DeshredUpdateOneof::DeshredTransaction(
                SubscribeUpdateDeshredTransaction {
                    transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                        signature: vec![1, 2, 3, 4],
                        is_vote: false,
                        transaction: None,
                        loaded_writable_addresses: Vec::new(),
                        loaded_readonly_addresses: Vec::new(),
                    }),
                    slot: 42,
                },
            )),
        };

        let signature = deshred_signature_from_update(
            update.update_oneof.as_ref().expect("update should exist"),
        );
        assert_eq!(signature.as_deref(), Some("2VfUX"));
    }

    #[test]
    fn build_deshred_ping_request_sets_ping_id() {
        let request = build_deshred_ping_request(7);

        assert!(request.deshred_transactions.is_empty());
        assert_eq!(request.ping.expect("ping should exist").id, 7);
    }

    #[test]
    fn deshred_account_matching_checks_static_and_loaded_addresses() {
        let watched = WatchedAccounts::new(&[
            "11111111111111111111111111111111".to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ])
        .expect("accounts should parse");

        let static_match = SubscribeUpdateDeshredTransactionInfo {
            signature: vec![1],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![1]],
                message: Some(Message {
                    account_keys: vec![vec![0; 32], vec![1; 32]],
                    ..Default::default()
                }),
            }),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
        };
        let loaded_match = SubscribeUpdateDeshredTransactionInfo {
            signature: vec![2],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![2]],
                message: Some(Message {
                    account_keys: vec![vec![9; 32]],
                    ..Default::default()
                }),
            }),
            loaded_writable_addresses: vec![
                bs58::decode("11111111111111111111111111111111")
                    .into_vec()
                    .expect("base58 pubkey should decode"),
            ],
            loaded_readonly_addresses: Vec::new(),
        };
        let no_match = SubscribeUpdateDeshredTransactionInfo {
            signature: vec![3],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![3]],
                message: Some(Message {
                    account_keys: vec![vec![9; 32]],
                    ..Default::default()
                }),
            }),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
        };

        assert!(deshred_matches_watched_accounts(&static_match, &watched));
        assert!(deshred_matches_watched_accounts(&loaded_match, &watched));
        assert!(!deshred_matches_watched_accounts(&no_match, &watched));
    }
}
