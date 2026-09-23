# Pump prospective collector validation — 23 September 2026

The standard Helius WebSocket endpoint delivers complete Pump Create/Trade event
fields directly. A transaction fetch per trade is unnecessary. However, truncated
logs omit some events, including migrations, so selective transaction recovery is
required. This is a research capture with visible gaps, not proof of total chain coverage.

The collector and one DigitalOcean Worker spec are prepared. The owner-approved
`fumppun` Space in `lon1` has passed live uploads and readback. No cloud Worker or
24-hour prospective run has started. The owner will connect the repository and
create the Worker in DigitalOcean directly.

## Demonstrated path

`wss://mainnet.helius-rpc.com/?api-key=[REDACTED]`

```json
{"jsonrpc":"2.0","id":1,"method":"logsSubscribe","params":[{"mentions":["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]},{"commitment":"confirmed"}]}
```

The invocation stack identifies Pump-owned `Program data:` log entries. The
Base64 event bytes are decoded with the vendored official Pump IDL, SHA-256
`ffe966c42f1af41652ee753fe2f1e3f7cd4077d7e6f49faf3138959c8b56064b`.
The source URL, retrieval time and byte count are in `sources/manifest.json`.
The decoder rejects unknown/truncated layouts rather than ignoring trailing bytes.

All decoded fields are retained, including amounts, reserves, mint/user/creator,
timestamps, fees and mode fields when present in the event type. Local historical
fixtures and selected finalized transactions confirmed full-field equality between
direct logs and the previously validated CPI-event path. Native integer values
are not converted into trading metrics.

This uses the standard method, even though Helius now describes its unified
WebSocket backend as LaserStream. [Helius WebSocket documentation](https://www.helius.dev/docs/rpc/websocket).

## Five-minute direct-log proof

Measured receiving interval: **19:19:40.332204–19:24:39.976328 UTC**, 299.644 seconds.

| Measurement | Observed |
| --- | ---: |
| Notifications | 85,550 |
| Successful / failed transaction notifications | 21,750 / 63,800 |
| Create / Trade / Complete events | 140 / 16,016 / 6 |
| Migration events directly in logs | 0 |
| All named Pump events, including ancillary types | 18,791 |
| Unknown discriminators / decode failures | 0 / 0 |
| Successful transactions with truncated logs | 196 (0.90%) |
| Average / peak messages per second | 285.51 / 1,022 |
| Average / peak target events per second | 53.94 / 435 |
| Received application-message bytes | 116,829,932 (116.83 MB) |
| Connection losses | 0 |
| Transaction fetches during direct proof | 0 |

Target events mean Create, Trade, Complete and CompletePumpAmmMigration. Peaks
use fixed one-second bins. Received bytes are UTF-8 JSON after WebSocket
decompression; transport framing/handshake bytes are excluded. Rates use the
active interval, not the extra five seconds spent closing the socket.

Evidence: `data/ws-proof-20260923/{subscription.json,notifications.jsonl.gz,
events.jsonl.gz,coverage.jsonl,summary.json,measured_metrics.json}`.
`measured_metrics.json` corrects the close-handshake denominator in the original summary.

## Fixed Bitquery comparison

Interval: **19:20:40 inclusive–19:20:50 exclusive UTC**, fixed from the first
subscription acknowledgment before inspecting results. One Bitquery recent-data
query returned 505 instruction rows; its 5,000-row limit was not reached.

| Target event | Direct WSS | Bitquery |
| --- | ---: | ---: |
| Create | 4 | 4 |
| Trade | 449 | 453 |
| Complete | 2 | 2 |
| Migration | 0 | 2 |
| Total | 455 | 461 |

All 455 direct events matched exact decoded event bytes; there were no WSS-only
events. All six Bitquery-only events occurred in transactions whose WSS
notifications arrived with truncated logs. Seven selected finalized transaction
checks confirmed matching log arrays and full Create/buy/sell/completion fields;
selected missing trade/migration events were available in CPI instruction data.
This explains the differences without treating either provider as ground truth.
The comparison uses event timestamps on the log side and indexed block time on
the Bitquery side; no boundary difference was found in this interval.

Evidence: `data/ws-proof-20260923/validation/{comparison.json,
difference_causes.json,selective_validation.json,request_results.jsonl}`.
No ongoing Bitquery usage is required by the collector.

A later [simultaneous subscription comparison](2026-09-23-subscription-comparison.md)
found that Helius's successful-only full transaction feed used 3.25 times more
bytes. Standard logs plus selective recovery remain the chosen deployment path.

## Durable Worker validation and Mayhem filtering

The first unfiltered five-minute Worker test uploaded 26 manifests, read back its
preflight object and flushed all local chunks on a real SIGTERM. It exposed an
enrichment backlog: 51 queued transactions remained after the earlier 30-second
drain. That run is marked incomplete. The collector now uses two bounded
asynchronous recovery consumers and a 45-second drain, and records unresolved
transaction identities explicitly.

The Mayhem-filtered Worker ran **19:51:06.011409–19:56:04.793397 UTC**
(298.782 seconds between first/last notification), then finished enrichment and
upload by 19:56:35.289188 UTC. A real SIGTERM was received.

| Measurement | Observed |
| --- | ---: |
| Messages / received bytes | 102,164 / 117,533,076 |
| Average / peak messages per second | 341.93 / 1,199 |
| Direct target events before filtering, average / peak per second | 40.74 / 123 |
| Retained Create / Trade / Complete / Migration events | 175 / 8,914 / 3 / 3 |
| Retained target events, average per second | 30.44 |
| Mayhem-only transactions excluded from raw storage | 3,343 |
| Known Mayhem decoded events excluded | 3,417 |
| All-Mayhem enrichments excluded | 5 |
| Successful transactions with truncated logs | 185 |
| Selected transaction fetches / resolved jobs | 203 / 203 |
| Unresolved recovery / decode failures / connection losses | 0 / 0 / 0 |
| Durable manifests / remaining local chunks | 19 / 0 |
| Raw gzip / decoded gzip / coverage gzip | 5,997,680 / 4,731,542 / 43,761 bytes |
| Projected compressed dataset per day | **3.12 GB** (2.90 GiB) |
| Projected incoming data per day | 33.99 GB |
| Projected Helius credits per day | **738,453**, about 7.4% of 10 million |

Every uploaded chunk was read back; hashes and row counts matched. No duplicate
event IDs or known-Mayhem decoded rows remained. Five raw records still contained
known Mayhem evidence because of mixed/incomplete contents. Anonymous GET of the
existing preflight object returned HTTP 403. The final checkpoint's stream time
matched the last WebSocket receipt, not the later enrichment/upload time.

The live run's 203 recovery jobs included 18 unnecessary creator-metadata
migration checks. After the run, the trigger was narrowed to actual `Migrate`
and `MigrateV2` instructions, with a regression test. This small correction has
unit coverage; the live measurements above are from the preceding version
(`collector_sha256=8274c962b31bb3169b21423e9a83902379fc79ec3fdee0d9df91730f3a6426a4`).
Its cost estimate is conservative for the corrected trigger.

Durable prefix: `fumppun/pump-lab/validation-20260923T195104Z/`.
Local evidence: `data/validation-20260923T195104Z-summary.json`,
`data/validation-20260923T195104Z-verification.json`, and
`data/collector-mayhem-validation.log`.
The storage projection includes raw, events and coverage; small JSON manifest
and checkpoint overhead is excluded. Short intervals do not establish a stable
daily rate, and differences from the earlier unfiltered test also reflect
changing traffic; they are not a controlled estimate of Mayhem storage savings.

Known Mayhem events are excluded after decoding. Related events are excluded
where their mint is known to be Mayhem. Healthy all-Mayhem transactions are not
saved as raw logs. Mixed/incomplete transactions retain their full evidence, and
unknown mode is explicit. There is no launch/account lookup for every mint.
Because the standard subscription filters only by program address, these
exclusions reduce storage and later analysis, not incoming streaming charges.
[Solana logsSubscribe filter](https://solana.com/docs/rpc/websocket/logssubscribe).

## Operation, limits and deployment

See [deployment instructions](../deploy/README.md) and
[one-Worker App spec](../deploy/pump-worker.yaml). Runtime command:
`python -u collector.py`. No service, API, frontend, database or trading rules.

Raw, decoded events and coverage are separate gzip JSONL streams. Thirty-second
or 4 MiB chunks upload to Spaces with hashes and manifests; checkpoints advance
only after uploads. Queues, dedupe caches and enrichment attempts are bounded.
Backpressure, reconnects, restarts, failed decodes and incomplete enrichment are
explicit coverage records. SIGTERM drains and flushes. Stable event IDs support
offline dedupe across direct logs and selected finalized transactions.

The first startup persists a 24-hour deadline; restarting cannot extend it.
After the deadline the Worker idles to prevent automatic restart into another
run. Remove the cloud Worker afterwards to stop hosting charges. The deployed
limits are 40 GB received and 100,000 selective RPC attempts; durable counters
can lag around crashes, so these are not exact provider-billing caps.

Sixteen focused tests passed, covering historical decode equality, foreign-log
exclusion, schema failures, Mayhem/mixed/unknown cases, dedupe, restart deadlines,
stream coverage boundaries, reconnects and upload-failure/SIGTERM behavior.
Docker image build and App Platform deployment have not been run; the local
Docker daemon is unavailable. The actual Python process has been tested live.

Confirmed direct events are not all checked for finality. There is no automatic
gap backfill or global exactly-once guarantee. A stored gap or unresolved event
must not be interpreted as zero activity. Mode caches are bounded; uncertain
events remain labeled unknown. Local wall-clock receipt time is not chain time.

## Cost estimate under current access

The owner reports Helius Developer access: **10 million credits/month**, with
the remaining balance unknown. Helius lists this plan at $49/month, streaming
at 20 credits/MB uncompressed, and standard getTransaction at one credit.
The initial proof projects about **674,000 streaming + 57,000 selective RPC
credits/day**, approximately **0.73 million/day**, before changes in traffic or
retries. This needs no plan upgrade if that much allowance remains. Local Mayhem
filtering does not reduce billed incoming bytes. [Helius credit schedule](https://www.helius.dev/docs/billing/credits).

The proposed Worker is **$5/month**, billed for its active lifetime (roughly
$0.18 for 24 hours), with 50 GiB/month outbound transfer. Inbound streaming is
free; uploads count toward Worker outbound bandwidth. No traffic exemption to
Spaces is assumed. [DigitalOcean App Platform pricing](https://docs.digitalocean.com/products/app-platform/details/pricing/).

The approved Spaces subscription is **$5/month**, including 250 GiB storage;
additional storage costs $0.02/GiB-month. A single projected day's dataset fits
within the included allowance if capacity remains. Downloading data uses the
included 1,024 GiB/month Spaces outbound allowance. [DigitalOcean Spaces pricing](https://docs.digitalocean.com/products/spaces/details/pricing/).

Bitquery used one bounded comparison query under existing access. Its remaining
point balance is not exposed by the response, so an exact point cost is unknown;
no upgrade, paid overage or recurring query was enabled.
