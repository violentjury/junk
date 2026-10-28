use crate::cfg::Sim;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use jito_protos::bundle::{bundle_result::Result as Verdict, Bundle};
use jito_protos::convert::proto_packet_from_versioned_tx;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::searcher::{SendBundleRequest, SubscribeBundleResultsRequest};
use jito_searcher_client::get_searcher_client_auth;
use jito_searcher_client::token_authenticator::ClientInterceptor;
use serde::Deserialize;
use serde_json::{json, Value};
use solana_sdk::signature::Keypair;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use thiserror::Error;
use tonic::{codegen::InterceptedService, transport::Channel};

pub type Searcher = SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>;
pub type Sent = (SimOutcome, Option<String>);

#[derive(Debug, Error)]
pub enum SimError {
    #[error("nothing to simulate")]
    Empty,
    #[error("serialize tx #{0}: {1}")]
    Encode(usize, bincode::Error),
    #[error("transport: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc {0}: {1}")]
    Rpc(i64, String),
    #[error("malformed response: {0}")]
    Shape(#[from] serde_json::Error),
    #[error("block engine: {0}")]
    Engine(#[from] jito_searcher_client::BlockEngineConnectionError),
    #[error("searcher rpc: {0}")]
    Grpc(#[from] tonic::Status),
}

#[derive(Debug, Clone, Default)]
pub struct SimOutcome {
    pub ok: bool,
    pub units: Vec<u64>,
    pub failed_sig: Option<String>,
    pub logs_tail: Vec<String>,
}

#[derive(Default)]
pub struct Totals {
    pub simulated: AtomicU64,
    pub sim_failed: AtomicU64,
    pub sent: AtomicU64,
    pub landed: AtomicU64,
    pub dropped: AtomicU64,
}

impl Totals {
    pub fn win_rate(&self) -> f64 {
        self.landed.load(Relaxed) as f64 / self.sent.load(Relaxed) as f64
    }

    pub fn sims_passed(&self) -> u64 {
        self.simulated.load(Relaxed) - self.sim_failed.load(Relaxed)
    }
}

pub struct SimSend {
    http: reqwest::Client,
    cfg: Sim,
    searcher: Searcher,
    pub tally: Arc<Totals>,
}

impl SimSend {
    pub async fn open(http: reqwest::Client, cfg: Sim, keypair: Keypair) -> Result<Self, SimError> {
        let searcher = get_searcher_client_auth(&cfg.block_engine_url, &Arc::new(keypair)).await?;
        Ok(Self {
            http,
            cfg,
            searcher,
            tally: Arc::default(),
        })
    }

    pub async fn watch_results(&self) -> Result<tokio::task::JoinHandle<()>, SimError> {
        let mut stream = self
            .searcher
            .clone()
            .subscribe_bundle_results(SubscribeBundleResultsRequest {})
            .await?
            .into_inner();
        let tally = Arc::clone(&self.tally);
        Ok(tokio::spawn(async move {
            let mut seen = HashSet::new();
            while let Ok(Some(res)) = stream.message().await {
                match res.result {
                    Some(Verdict::Finalized(_)) | Some(Verdict::Processed(_)) => {
                        if seen.insert(res.bundle_id.clone()) {
                            tally.landed.fetch_add(1, Relaxed);
                        }
                    }
                    Some(Verdict::Dropped(_)) | Some(Verdict::Rejected(_)) => {
                        tally.dropped.fetch_add(1, Relaxed);
                        tracing::debug!(id = %res.bundle_id, "bundle did not land");
                    }
                    Some(Verdict::Accepted(_)) | None => {}
                }
            }
        }))
    }

    pub async fn run(&self, txs: &[VersionedTransaction]) -> Result<Sent, SimError> {
        let sim = self.simulate(txs).await?;
        self.tally.simulated.fetch_add(1, Relaxed);
        if !sim.ok {
            self.tally.sim_failed.fetch_add(1, Relaxed);
            return Ok((sim, None));
        }
        let packets = txs.iter().map(proto_packet_from_versioned_tx).collect();
        let resp = self
            .searcher
            .clone()
            .send_bundle(SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets,
                }),
            })
            .await?;
        self.tally.sent.fetch_add(1, Relaxed);
        Ok((sim, Some(resp.into_inner().uuid)))
    }

    pub async fn simulate(&self, txs: &[VersionedTransaction]) -> Result<SimOutcome, SimError> {
        if txs.is_empty() {
            return Err(SimError::Empty);
        }
        let encoded = txs
            .iter()
            .enumerate()
            .map(|(i, tx)| {
                bincode::serialize(tx)
                    .map(|b| B64.encode(b))
                    .map_err(|e| SimError::Encode(i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "simulateBundle",
            "params": [
                { "encodedTransactions": encoded },
                {
                    "skipSigVerify": self.cfg.skip_sigverify,
                    "replaceRecentBlockhash": self.cfg.replace_recent_blockhash,
                },
            ],
        });
        let env: RpcEnvelope = self
            .http
            .post(&self.cfg.rpc_url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(e) = env.error {
            return Err(SimError::Rpc(e.code, e.message));
        }
        let mut result = env.result.unwrap_or(Value::Null);
        let value: SimValue = serde_json::from_value(result["value"].take())?;
        let units = value
            .transaction_results
            .iter()
            .map(|r| r.units_consumed.unwrap_or(0))
            .collect();
        let logs_tail = value
            .transaction_results
            .last()
            .and_then(|r| r.logs.as_deref())
            .map(|l| l[l.len().saturating_sub(12)..].to_vec())
            .unwrap_or_default();
        let failed_sig = match value.summary {
            Summary::Succeeded(_) => None,
            Summary::Failed { failed } => Some(failed.tx_signature.unwrap_or_default()),
        };
        Ok(SimOutcome {
            ok: failed_sig.is_none(),
            units,
            failed_sig,
            logs_tail,
        })
    }
}

#[derive(Deserialize)]
struct RpcEnvelope {
    result: Option<Value>,
    error: Option<Fault>,
}

#[derive(Deserialize)]
struct Fault {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SimValue {
    summary: Summary,
    #[serde(default)]
    transaction_results: Vec<TxResult>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Summary {
    Succeeded(String),
    Failed { failed: Failure },
}

#[derive(Deserialize)]
struct Failure {
    tx_signature: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxResult {
    logs: Option<Vec<String>>,
    units_consumed: Option<u64>,
}
