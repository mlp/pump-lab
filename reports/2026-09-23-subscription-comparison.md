# Concurrent Helius subscription comparison

**Keep standard `logsSubscribe` plus selective recovery.** The tested full
successful-transaction feed consumed 3.25 times as many incoming bytes.
Including one selective RPC per incomplete transaction, its projected credit
cost was about 2.96 times the current collector's cost in this interval.

Measured UTC interval: **2026-09-23 20:07:46.822600–20:12:47.002323**,
300.194 seconds. Both subscriptions shared one connection and ran concurrently.
Both used `confirmed`, the same official Pump program, the same existing Helius
Developer access and the pinned Pump IDL. No transaction RPC calls were made.

| Measurement | Standard logs | Full successful transactions |
| --- | ---: | ---: |
| Method | `logsSubscribe` | `transactionSubscribe` |
| Provider-side success filter | unavailable | `failed: false` |
| Notifications | 49,175 | 17,220 |
| Successful notifications | 17,219 | 17,220 |
| Failed notifications | 31,956 | 0 |
| Incoming uncompressed bytes | 83,944,185 | 272,659,735 |
| Projected streaming credits/day | 483,207 | 1,569,507 |
| Estimated selective-recovery credits/day | 47,489 | 0 |
| Estimated combined credits/day | **530,696** | **1,569,507** |
| Decode failures | 0 | 0 |
| Connection losses | 0 | 0 |

Full transactions used `encoding: jsonParsed`, `transactionDetails: full`,
`showRewards: false`, and `maxSupportedTransactionVersion: 1`. Their extra
transaction/account/balance/instruction metadata outweighed removing failures.
This is evidence for this tested configuration, not a claim that every possible
Helius product or encoding has been optimized.

Failed standard notifications accounted for 28,373,825 bytes (33.8% of its
stream). Removing them locally cannot save incoming credits. Helius documents
both WebSocket methods at 20 credits per decimal MB, and a standard
getTransaction call at one credit. Projections are arithmetic estimates from
application-message bytes, not Helius account billing telemetry.
[Helius credit schedule](https://www.helius.dev/docs/billing/credits).

## Coverage comparison

To avoid subscription/start/stop boundary effects, comparison used the common
interior slot range **449807024–449808155**, trimming two slots at each end.

- **17,137 successful signatures** appeared in both feeds; none appeared in
  only one feed within the comparison range.
- **12,619 target events** matched exact event bytes across direct logs and
  decoded transaction CPI instructions.
- The full feed contained **196 additional target events across 71 transactions**.
  Every one of those transactions had logs already flagged as incomplete by the
  current collector. There were no unexplained direct-log-only target events.
- This supports the existing selective recovery trigger; it does not establish
  global chain completeness or prove that every future RPC recovery will succeed.

The whole capture had 165 successful transactions selected by that trigger.
Standard-plus-recovery estimates assume one successful RPC attempt each; retries
and traffic changes can increase usage. One extra successful notification at a
capture boundary was excluded by the common-interior comparison.

## Decision and evidence

Deploy the validated standard-log Worker with Mayhem exclusion, selective
recovery, durable coverage records and its fixed 24-hour deadline. The observed
daily projection varied from about **0.53 million to 0.74 million credits** across
the local tests. A full 10-million-credit balance would cover approximately
13–19 days at those rates, not a continuous 30-day month. This deployment is
only a 24-hour research run.

Local evidence directory: `data/subscription-comparison-20260923T220746/`.
The directory name uses local time; all timestamps inside the files are UTC.
`requests.json`, `capture.json`, `messages.jsonl.gz`, and `comparison.json`
preserve request parameters, coverage, byte counts, raw evidence and differences.
They are deliberately outside the published repository. The repeatable bounded
measurement script is `compare_subscriptions.py`.
