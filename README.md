# GeyserBench

GeyserBench benchmarks the speed and reliability of Solana gRPC-compatible data feeds so you can compare providers with consistent metrics.

## Highlights

- Benchmark multiple feeds at once (Yellowstone, aRPC, Thor, Shredstream, Jetstream, Helius Preconfirmations, and custom gRPC endpoints)
- Track first-detection share, latency percentiles (P50/P95/P99), valid transaction counts, and backfill events
- Stream results to the SolStack backend for shareable reports, or keep runs local with a single flag
- Generate a ready-to-edit TOML config on first launch; supply auth tokens and endpoints without code changes

## Installation

### Prebuilt binaries
- Download the latest release from the [GitHub releases page](https://github.com/solstackapp/geyserbench/releases) and place the binary on your `PATH`.

### Build from source
```bash
cargo build --release
```
The compiled binary is written to `target/release/geyserbench`.

## Quick Start

1. Run the binary once to scaffold `config.toml` in the current directory:
   ```bash
   ./target/release/geyserbench
   ```
2. Edit `config.toml` with the accounts, endpoints, and tokens you want to test.
3. Run the benchmark. Use `--config <PATH>` to point at another file or `--private` to disable backend streaming:
   ```bash
   ./target/release/geyserbench --private
   ```

During a run, GeyserBench prints progress updates followed by a side-by-side comparison table. When streaming is enabled the tool also returns a shareable link once the backend finalizes the report.

## Example Output

![CLI output showing endpoint win rates and latency percentiles](./assets/cli_screenshot.png)

## Configuration Reference

`geyserbench` reads a single TOML file that defines the run parameters and endpoints:

```toml
[config]
transactions = 1000
account = ["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"]
commitment = "processed"  # processed | confirmed | finalized

[[endpoint]]
name = "Jito Shredstream"
url = "http://localhost:10000"
kind = "shredstream"

[[endpoint]]
name = "Corvus aRPC"
url = "https://fra.corvus-labs.io:20202"
kind = "arpc"

[[endpoint]]
name = "Corvus gRPC"
url = "https://fra.corvus-labs.io:10101"
x_token = "optional-auth-token"
kind = "yellowstone"

[[endpoint]]
name = "Local Yellowstone UDS"
url = "unix:///var/run/geyser.sock"
x_token = "optional-auth-token"
kind = "yellowstone"

[[endpoint]]
name = "Local Yellowstone Deshred"
url = "https://fra.corvus-labs.io:10101"
x_token = "optional-auth-token"
kind = "yellowstone_deshred"

# Optional Helius reference stream. Prefer x_token so the key does not appear in
# copied URLs. Omit region_include (or use []) to receive every Helius region.
[[endpoint]]
name = "Helius Preconf"
url = "wss://beta.helius-rpc.com/"
x_token = "YOUR_HELIUS_API_KEY"
region_include = ["sgp", "tyo"]
kind = "helius_preconf"
```

- `config.transactions` sets how many signatures to evaluate (backend streaming automatically disables itself for extremely large runs).
- `config.account` is the list of pubkeys monitored for transactions during the benchmark. A transaction matches if it contains any listed pubkey.
- `config.commitment` accepts `processed`, `confirmed`, or `finalized`.
- Repeat `[[endpoint]]` blocks for each feed. Supported `kind` values: `yellowstone`, `yellowstone_deshred`, `arpc`, `thor`, `shredstream`, `shreder`, `jetstream`, and `helius_preconf`. `x_token` is optional except when a Helius API key is not already present in the URL.
- `kind = "yellowstone"` and `kind = "yellowstone_deshred"` both support `http://`, `https://`, and `unix:///absolute/path.sock`. UDS does not require a new `kind`.
- `kind = "yellowstone_deshred"` uses Yellowstone's `SubscribeDeshred` stream and participates in the same comparison and metrics output as other endpoints.
- At most one `kind = "helius_preconf"` endpoint is supported. It must use the official `wss://beta.helius-rpc.com/` endpoint; `config.account` is sent as Helius `accountInclude`, `failed` is set to `false`, and account lists are limited to 500 entries.
- Optional `region_include` is sent as Helius `regionInclude`. Supported codes are `slc`, `fra`, `lon`, `pit`, `sgp`, `ewr`, `tyo`, `ams`, `dal`, `dub`, `mia`, `lax`, `iad`, and `sea`.
- `config.commitment` is still recorded for the run, but Yellowstone deshred subscriptions do not send a commitment field because the upstream RPC does not accept one.
- Bare socket paths like `/var/run/geyser.sock` are rejected; use the explicit `unix:///...` form.

## Helius Preconf Comparison

Helius documents that Preconfirmation coverage is not continuous, so the Preconf stream is not included in the ordinary all-endpoint intersection or in `config.transactions`. Instead, GeyserBench prints a separate table whose baseline `N` is exactly the unique signatures received from `preconfSubscribe`.

Each ordinary endpoint is joined independently against those signatures. The table reports `Matched`, `Missing` (not observed by benchmark end), coverage, wins, and signed latency percentiles using only matched signatures. `Δ = endpoint receipt - preconf receipt`: a positive value means Preconf was faster, a negative value means the endpoint was faster, and zero is a tie. Missing signatures are never counted as wins or as zero-latency samples.

Preconfirmation notifications are binary WebSocket frames with an 18-byte header followed by `bincode(VersionedTransaction)`; GeyserBench compares the first transaction signature. Receipt time is captured before decoding. A Preconfirmation remains an early signal rather than a landing guarantee.

Coverage also reflects filter semantics: Helius applies `accountInclude` server-side, while some ordinary providers inspect only the account keys exposed by their stream. Transactions matching solely through address lookup tables can therefore appear as missing for those providers.

## CLI Options

- `--config <PATH>` &mdash; load configuration from a different TOML file (defaults to `config.toml`).
- `--private` &mdash; keep results local by skipping the streaming backend, even when the run qualifies for sharing.
- `-h`, `--help` &mdash; show usage information.

Streaming is enabled by default for standard-sized runs and publishes to `https://runs.solstack.app`. You can always opt out with `--private` or by configuring the backend section to point at your own infrastructure.
