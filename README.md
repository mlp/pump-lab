# Pump Lab collector

One Python research collector for a **24-hour prospective Pump capture**.
Runs as one DigitalOcean App Platform background Worker and writes private gzip
JSONL chunks to the approved `fumppun` Space in `lon1`.

It decodes official Pump event logs, selectively recovers incomplete transactions,
excludes known Mayhem activity where possible, and records coverage gaps.
There is no API, frontend, database or trading integration.

- [DigitalOcean setup](deploy/README.md)
- [Live validation and cost evidence](reports/2026-09-23-live-collector.md)
- [Why standard logs cost less in our comparison](reports/2026-09-23-subscription-comparison.md)
- App spec: `deploy/pump-worker.yaml`
- Dockerfile: `Dockerfile`
- Run command: `python -u collector.py`

Secrets belong in DigitalOcean **encrypted runtime environment variables**.
Never commit `.env.local`. The repository contains collector source, the pinned
official Pump IDL, and documentation. Captured datasets stay outside Git.

The first startup fixes a 24-hour deadline in Spaces. Restarts keep that deadline.
At the end the Worker flushes and idles; delete it afterwards to stop hosting charges.

The historical analysis workspace and its saved data remain local. Its original
instructions are preserved locally in `analysis/HISTORICAL-README.md`.
