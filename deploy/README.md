# Pump research Worker

Prepared for one 24-hour run. The owner will connect this repository to App
Platform and create the $5/month Worker. No cloud Worker has been created by
this task. The owner-approved Space is `fumppun` in `lon1`.

## Quick setup in App Platform

1. Create an app from GitHub repository `mlp/pump-lab`, branch `master`.
2. Configure **one Worker**, with Dockerfile path `Dockerfile`, source
   directory `/`, and run command `python -u collector.py`. Remove any automatically
   proposed web service. No port, health endpoint or database is needed.
3. Choose **London**, one **512 MiB shared CPU** instance at **$5/month**. Disable
   automatic deployment on push for this fixed research run.
4. Supply the three secret values from your local environment, mark each
   **Encrypt**, and use runtime scope. The Dockerfile already supplies the approved
   bucket, region, prefix, run ID and Mayhem setting. `deploy/pump-worker.yaml`
   repeats these settings explicitly. Set the termination grace period to
   **180 seconds** through the App spec.
5. Deploy. Runtime logs should show `worker_started`, `subscribed`, then
   `chunk_durable`. Confirm that the corresponding prefix appears in `fumppun`.

You can use `deploy/pump-worker.yaml` as the complete configuration instead of
entering settings individually. The secret values are intentionally blank.

Runtime: Python 3.12, one process, one standard Pump `logsSubscribe` connection.
The two asynchronous enrichment consumers run inside that process; they only
fetch transactions whose logs are incomplete. No Bitquery key is used by the Worker.

## Files and command

- `collector.py`, `ws_probe.py`, `decode.py`
- `sources/pump.json` (pinned official IDL)
- `requirements-collector.txt`
- `Dockerfile`, `.dockerignore`
- `deploy/pump-worker.yaml`

Run command: `python -u collector.py`.

Local dependencies: `.venv/bin/python -m pip install -r requirements-collector.txt`.
The current venv was installed using `uv pip install --python .venv/bin/python -r requirements-collector.txt`.
For another paid, bounded local validation with the configured credentials:
`.venv/bin/python validate_collector_live.py` (five minutes; writes to the Space).
Tests: `.venv/bin/python -m unittest test_collector.py test_ws_probe.py`.
This engineering test command is for the original local research workspace;
those fixture-based tests and saved historical fixtures are not in the deployment-only
repository. The standalone live validation scripts are included, along with
network-free startup checks: `.venv/bin/python -m unittest test_collector_config.py`.

## DigitalOcean configuration

Use the App spec in this directory. It contains exactly one background Worker,
London region `lon`, one `apps-s-1vcpu-0.5gb` instance (512 MiB, $5/month), with
180 seconds for graceful termination. No HTTP port, service or database.

The GitHub source is the existing public repository `mlp/pump-lab`, branch
`master`. The deployment commit contains collector source, the pinned IDL and
documentation. `.env.local`, captured data, research attachments and API responses
are excluded. Historical analysis remains in the local workspace.

Set these component variables as **encrypted secrets**, runtime scope:

| Secret | Value source |
| --- | --- |
| `HELIUS` | Existing Helius API key |
| `SPACES_ACCESS_KEY_ID` | Scoped fumppun Spaces access key |
| `SPACES_SECRET_ACCESS_KEY` | Matching Spaces secret |

App-level secrets are also inherited by the Worker. A component variable with
the same name takes precedence, including a blank value; avoid blank duplicates.

Their values are deliberately omitted from the App spec. Enter them in the
DigitalOcean environment panel before deployment. `.env.local` is only for local
validation and is excluded from the Docker context. No DigitalOcean API token
is required for the owner to connect and deploy the repository in the control panel.

If startup reports `MissingEnvironment`, its `missing_environment_variables`
list names the settings to correct. It never prints their values. A generic
`KeyError` from the first deployment may mean the non-secret settings were not
entered; deploy the latest commit to pick up the Dockerfile defaults. App Platform
environment values override image defaults, so remove or correct blank overrides.

Non-secret settings already in the spec:

| Setting | Value |
| --- | --- |
| `SPACES_BUCKET` / `SPACES_REGION` | `fumppun` / `lon1` |
| `SPACES_PREFIX` | `pump-lab/prospective` |
| `PUMP_RUN_ID` | `pump-24h-20260923` |
| `PUMP_EXCLUDE_MAYHEM` | `1` |
| `PUMP_MAX_RECEIVED_BYTES` | `40000000000` |
| `PUMP_MAX_ENRICHMENT_REQUESTS` | `100000` |

The first startup fixes a deadline 24 hours later in the durable checkpoint.
Restarts keep that deadline. Reusing the run ID cannot start a fresh 24 hours.
Only use a new run ID for another expressly intended run. Do not run two instances
under one run ID. A local validation supplies `PUMP_STOP_AT_UTC` for its shorter
deadline; leave it unset for the prospective run.

At the deadline or stream byte limit, the process flushes and idles so App
Platform cannot restart it into another capture. **An idle Worker still costs
money; delete the Worker/app after the run.** Keep the Space and its dataset.

## Dataset and failure behavior

Objects are written under `<prefix>/<run_id>/<boot_id>/<sequence>.*`:

- `raw.jsonl.gz`: successful raw log notifications; selected raw transactions.
- `events.jsonl.gz`: decoded fields, original event bytes, signature, slot,
  receipt timestamp, log/CPI ordering, source and commitment, pinned IDL hash.
- `coverage.jsonl.gz`: connections, possible gaps, decode problems, enrichment
  outcomes, intentional exclusions, traffic counters and final summary.
- `manifest.json`: row counts, compressed file sizes and SHA-256 hashes.
- `<prefix>/<run_id>/checkpoint.json`: durable progress and fixed stop time.

A manifest becomes complete after its referenced objects upload. The checkpoint
advances only after that; local chunks are removed last. Objects use private S3
defaults. There is no public/CDN upload. Validation reads every completed chunk
back and verifies its hash and row count.

Chunks rotate at 30 seconds or 4 MiB of combined uncompressed JSONL, with a
single-record overshoot. The upload queue holds two chunks plus the active and
uploading chunk. Storage backpressure closes the stream and records a possible
gap; it does not keep growing local disk. Uploads retry with backoff to 30 seconds.
SIGTERM allows 45 seconds for enrichment and 75 seconds for final uploads.

Known Mayhem Create/Trade events identify their mode directly in the IDL fields.
The collector excludes those events and related events with a known matching
mint. Healthy transactions whose market events are all known Mayhem are omitted
from raw storage. Full raw evidence for mixed or incomplete transactions is kept;
unknown classifications are explicit. Therefore this is **best-effort Mayhem
exclusion**, not a claim that no Mayhem bytes remain. Incoming Helius bytes are
unchanged by this local filter. Exclusion counters make intentional filtering visible.

Event identity is signature + normalized event payload SHA-256 + occurrence of
that identical payload within the transaction. Log and CPI recovery share IDs.
Memory dedupe is bounded, and restart restores the last two event chunks.
Use `event_id` for final offline deduplication; arbitrary replay is not globally
exactly-once. Log ordering is within a transaction, not block-wide transaction order.

Disconnects and restarts produce possible-gap intervals. There is no automatic
historical backfill. Pending incomplete transactions are identifiable in coverage;
unresolved enrichment stays a data-quality gap. Missing data is not zero activity.
Confirmed direct events are not all rechecked for finality; selected enrichments
are finalized and carry separate provenance. Most logs provide complete fields,
but this collector does not claim lossless chain coverage.

The 40 GB / 100,000 RPC limits constrain this run's usage. Checkpointed counters
can lag around crashes, and provider billing is authoritative. They are not an
account-wide credit balance check. No provider upgrades or paid overage settings
are enabled by this code.
