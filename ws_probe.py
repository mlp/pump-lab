"""Bounded local proof of standard logsSubscribe; not a deployed collector."""
import argparse
import asyncio
import base64
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import time
from urllib.parse import quote

from websockets.asyncio.client import connect
from decode import IDL, IDL_SHA256, decode_event

PROGRAM = IDL['address']
TARGETS = {'CreateEvent', 'TradeEvent', 'CompleteEvent', 'CompletePumpAmmMigrationEvent'}
ROOT = Path(__file__).resolve().parent


def utc():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def config():
    values = dict(os.environ)
    path = ROOT / '.env.local'
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.removeprefix('export ').split('=', 1)
                values.setdefault(key.strip(), value.strip().strip('\"\''))
    return values


def parse_logs(logs):
    """Decode only data emitted in a Pump invocation, retaining log indices."""
    stack, events, failures = [], [], []
    stats = Counter()
    for index, line in enumerate(logs):
        match = re.fullmatch(r'Program (\w+) invoke \[(\d+)\]', line)
        if match:
            depth = int(match[2])
            stack = stack[:depth-1]
            stack.append(match[1])
            continue
        match = re.match(r'Program (\w+) (success|failed:)', line)
        if match:
            if stack and stack[-1] == match[1]:
                stack.pop()
            else:
                stats['unmatched_return'] += 1
            continue
        if 'Log truncated' in line:
            stats['truncated_logs'] += 1
        if not line.startswith('Program data: '):
            continue
        stats['program_data_lines'] += 1
        if not stack or stack[-1] != PROGRAM:
            stats['foreign_or_unattributed_data_lines'] += 1
            continue
        stats['pump_data_lines'] += 1
        for part, encoded in enumerate(line.removeprefix('Program data: ').split()):
            try:
                raw = base64.b64decode(encoded, validate=True)
                decoded = decode_event(raw)
                if decoded is None:
                    failures.append(dict(log_index=index, data_part=part,
                        reason='unknown_discriminator', discriminator_hex=raw[:8].hex()))
                else:
                    name, fields = decoded
                    events.append(dict(event_name=name, fields=fields, log_index=index,
                        data_part=part, event_ordinal=len(events), event_base64=encoded,
                        event_sha256=hashlib.sha256(raw).hexdigest()))
            except Exception as exc:
                failures.append(dict(log_index=index, data_part=part,
                    reason='decode_failure', error_type=type(exc).__name__))
    return events, failures, dict(stats)


async def capture(seconds, output):
    output.mkdir(parents=True, exist_ok=False)
    settings = config()
    endpoint = settings.get('PUMP_WSS_URL') or 'wss://mainnet.helius-rpc.com/?api-key=' + quote(settings['HELIUS'], safe='')
    if not endpoint.startswith('wss://'):
        raise ValueError('A TLS WebSocket endpoint is required')
    stopped = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stopped.set)
    request = dict(jsonrpc='2.0', id=1, method='logsSubscribe',
        params=[{'mentions': [PROGRAM]}, {'commitment': 'confirmed'}])
    (output/'subscription.json').write_text(json.dumps(request, indent=2)+'\n')
    counters, event_counts, message_bins, event_bins = Counter(), Counter(), Counter(), Counter()
    seen, connections = set(), []
    start = time.monotonic()
    started_at = utc()
    deadline = start+seconds
    byte_cap = 256*1024*1024
    with gzip.open(output/'notifications.jsonl.gz', 'wt', compresslevel=6) as raw_file, \
            gzip.open(output/'events.jsonl.gz', 'wt', compresslevel=6) as events_file, \
            (output/'coverage.jsonl').open('w') as coverage:
        def status(kind, **extra):
            record = dict(kind=kind, at_utc=utc(), **extra)
            coverage.write(json.dumps(record)+'\n'); coverage.flush()
            print(json.dumps(record), flush=True)
        status('probe_started', duration_limit_seconds=seconds, program=PROGRAM,
               endpoint='wss://mainnet.helius-rpc.com/?api-key=[REDACTED]', idl_sha256=IDL_SHA256)
        attempt = 0
        while time.monotonic() < deadline and not stopped.is_set():
            attempt += 1
            counters['connection_attempts'] += 1
            connected_at = None
            try:
                async with connect(endpoint, open_timeout=15, close_timeout=5,
                                   ping_interval=20, ping_timeout=20, max_size=8*1024*1024,
                                   max_queue=32) as ws:
                    await ws.send(json.dumps(request))
                    ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                    if ack.get('id') != 1 or not isinstance(ack.get('result'), int):
                        status('subscription_rejected', error_code=ack.get('error', {}).get('code'))
                        counters['subscription_rejections'] += 1
                        break
                    connected_at = utc()
                    counters['subscriptions_acknowledged'] += 1
                    status('subscribed', attempt=attempt, subscription_id=ack['result'])
                    next_status = time.monotonic()+30
                    while time.monotonic() < deadline and not stopped.is_set():
                        try:
                            wire = await asyncio.wait_for(ws.recv(), timeout=min(1, max(.01, deadline-time.monotonic())))
                        except asyncio.TimeoutError:
                            continue
                        received = utc()
                        tick = int(time.monotonic()-start)
                        size = len(wire.encode() if isinstance(wire, str) else wire)
                        counters['received_message_bytes'] += size
                        counters['messages'] += 1; message_bins[tick] += 1
                        message = json.loads(wire)
                        raw_file.write(json.dumps(dict(received_at_utc=received,
                            connection_attempt=attempt, wire_bytes=size, message=message), separators=(',', ':'))+'\n')
                        if message.get('method') != 'logsNotification':
                            counters['other_messages'] += 1
                            continue
                        result = message['params']['result']; value = result['value']
                        signature = value['signature']; slot = result['context']['slot']
                        if value['err'] is not None:
                            counters['failed_transaction_notifications'] += 1
                        else:
                            counters['successful_transaction_notifications'] += 1
                            events, failures, stats = parse_logs(value['logs'])
                            counters.update(stats)
                            if not events:counters['successful_without_decoded_event'] += 1
                            for failure in failures:
                                counters[failure['reason']] += 1
                                events_file.write(json.dumps(dict(record_type='decode_failure', signature=signature,
                                    slot=slot, received_at_utc=received, **failure))+'\n')
                            for event in events:
                                identity = f"{signature}:{event['log_index']}:{event['data_part']}"
                                if identity in seen:
                                    counters['duplicate_events'] += 1
                                    continue
                                seen.add(identity)
                                event_counts[event['event_name']] += 1
                                if event['event_name'] in TARGETS:
                                    event_bins[tick] += 1
                                events_file.write(json.dumps(dict(record_type='decoded_event', event_id=identity,
                                    signature=signature, slot=slot, received_at_utc=received,
                                    commitment='confirmed', idl_sha256=IDL_SHA256, **event), separators=(',', ':'))+'\n')
                        if counters['received_message_bytes'] >= byte_cap:
                            status('byte_cap_reached', byte_cap=byte_cap); stopped.set()
                        if time.monotonic() >= next_status:
                            raw_file.flush(); events_file.flush()
                            status('progress', counters=dict(counters), events=dict(event_counts))
                            next_status=time.monotonic()+30
                    status('bounded_capture_finished')
            except Exception as exc:
                counters['connection_errors'] += 1
                status('connection_error', error_type=type(exc).__name__, attempt=attempt)
                if attempt >= 4:break
                await asyncio.sleep(min(2**(attempt-1), 8, max(0, deadline-time.monotonic())))
            finally:
                if connected_at:connections.append(dict(start_utc=connected_at, end_utc=utc()))
        status('probe_stopped')
    elapsed = time.monotonic()-start
    summary = dict(started_at_utc=started_at, ended_at_utc=utc(), elapsed_seconds=elapsed,
        requested_seconds=seconds, commitment='confirmed', program=PROGRAM, idl_sha256=IDL_SHA256,
        counters=dict(counters), decoded_events=dict(event_counts), connection_intervals=connections,
        target_events=sum(event_counts[n] for n in TARGETS),
        average_messages_per_second=counters['messages']/elapsed,
        peak_messages_in_one_second=max(message_bins.values(), default=0),
        average_target_events_per_second=sum(event_counts[n] for n in TARGETS)/elapsed,
        peak_target_events_in_one_second=max(event_bins.values(), default=0),
        bytes_per_day_at_observed_rate=counters['received_message_bytes']/elapsed*86400,
        raw_gzip_bytes=(output/'notifications.jsonl.gz').stat().st_size,
        decoded_gzip_bytes=(output/'events.jsonl.gz').stat().st_size,
        full_transaction_fetches=0,
        limits='Local bounded proof only; confirmed events are not finalized; no claim of complete coverage or transaction index within slot.')
    (output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=300, choices=range(300,601))
    parser.add_argument('--output', type=Path, required=True)
    args=parser.parse_args()
    try:
        asyncio.run(capture(args.seconds,args.output))
    except Exception as exc:
        print(json.dumps({'fatal_error_type':type(exc).__name__}), flush=True)
        raise SystemExit(1)
