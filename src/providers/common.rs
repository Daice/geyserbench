use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, bail};
use crossbeam_queue::ArrayQueue;
use solana_pubkey::Pubkey;
use tracing::{error, warn};

use crate::{
    backend::{SignatureEnvelope, SignatureObservation},
    utils::{Comparator, TransactionData},
};

#[derive(Default)]
pub struct TransactionAccumulator {
    entries: HashMap<String, TransactionData>,
}

#[derive(Clone, Debug)]
pub struct WatchedAccounts {
    filters: Vec<String>,
    pubkeys: Vec<Pubkey>,
}

impl WatchedAccounts {
    pub fn new(accounts: &[String]) -> Result<Self> {
        if accounts.is_empty() {
            bail!("config.account must contain at least one pubkey");
        }

        let mut pubkeys = Vec::with_capacity(accounts.len());
        for (index, account) in accounts.iter().enumerate() {
            let pubkey = account
                .parse::<Pubkey>()
                .with_context(|| format!("invalid pubkey in config.account[{index}]: {account}"))?;
            pubkeys.push(pubkey);
        }

        Ok(Self {
            filters: accounts.to_vec(),
            pubkeys,
        })
    }

    pub fn filters(&self) -> &[String] {
        &self.filters
    }

    pub fn matches_pubkey(&self, pubkey: &Pubkey) -> bool {
        self.pubkeys.iter().any(|candidate| candidate == pubkey)
    }

    pub fn matches_bytes(&self, bytes: &[u8]) -> bool {
        self.pubkeys
            .iter()
            .any(|candidate| candidate.as_ref() == bytes)
    }
}

impl TransactionAccumulator {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn record(&mut self, signature: String, data: TransactionData) -> bool {
        use std::collections::hash_map::Entry;

        match self.entries.entry(signature) {
            Entry::Vacant(entry) => {
                entry.insert(data);
                true
            }
            Entry::Occupied(mut entry) => {
                if data.elapsed_since_start < entry.get().elapsed_since_start {
                    entry.insert(data);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn into_inner(self) -> HashMap<String, TransactionData> {
        self.entries
    }
}

pub fn fatal_connection_error(endpoint: &str, err: impl std::fmt::Display) -> ! {
    error!(endpoint = endpoint, error = %err, "Failed to connect to endpoint");
    eprintln!("Failed to connect to endpoint {}: {}", endpoint, err);
    std::process::exit(1);
}

pub fn build_signature_envelope(
    comparator: &Arc<Comparator>,
    endpoint: &str,
    signature: &str,
    data: TransactionData,
    total_producers: usize,
) -> Option<SignatureEnvelope> {
    comparator
        .record_observation(endpoint, signature, data, total_producers)
        .map(|observations| {
            let mut payload = observations
                .into_iter()
                .map(|(endpoint, tx_data)| SignatureObservation {
                    endpoint,
                    timestamp: tx_data.wallclock_secs,
                    backfilled: tx_data.wallclock_secs < tx_data.start_wallclock_secs,
                })
                .collect::<Vec<_>>();
            payload.sort_by(|lhs, rhs| lhs.endpoint.cmp(&rhs.endpoint));
            SignatureEnvelope {
                signature: signature.to_owned(),
                observations: payload,
            }
        })
}

pub fn enqueue_signature(
    sender: &Arc<ArrayQueue<SignatureEnvelope>>,
    endpoint: &str,
    signature: &str,
    envelope: SignatureEnvelope,
) {
    if sender.push(envelope).is_err() {
        warn!(endpoint = endpoint, signature = %signature, "Signature queue full; dropping observation");
    }
}

#[cfg(test)]
mod tests {
    use super::WatchedAccounts;
    use solana_pubkey::Pubkey;

    #[test]
    fn watched_accounts_match_any_pubkey_and_preserve_filters() {
        let filters = vec![
            "11111111111111111111111111111111".to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ];
        let watched = WatchedAccounts::new(&filters).expect("accounts should parse");
        let matched = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            .parse::<Pubkey>()
            .expect("valid pubkey");
        let unmatched = "Vote111111111111111111111111111111111111111"
            .parse::<Pubkey>()
            .expect("valid pubkey");

        assert_eq!(watched.filters(), filters.as_slice());
        assert!(watched.matches_pubkey(&matched));
        assert!(!watched.matches_pubkey(&unmatched));
    }

    #[test]
    fn watched_accounts_match_any_raw_pubkey_bytes() {
        let filters = vec![
            "11111111111111111111111111111111".to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ];
        let watched = WatchedAccounts::new(&filters).expect("accounts should parse");
        let matched = "11111111111111111111111111111111"
            .parse::<Pubkey>()
            .expect("valid pubkey");
        let unmatched = "Vote111111111111111111111111111111111111111"
            .parse::<Pubkey>()
            .expect("valid pubkey");
        let matched_bytes = matched.to_bytes();
        let unmatched_bytes = unmatched.to_bytes();

        assert!(watched.matches_bytes(matched_bytes.as_slice()));
        assert!(!watched.matches_bytes(unmatched_bytes.as_slice()));
    }
}
