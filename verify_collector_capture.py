"""Read back a bounded validation run from Spaces; no chain/provider requests."""
import argparse
from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path

from collector import Spaces
from ws_probe import TARGETS, config


def verify(path):
    summary=json.loads(path.read_text());settings=config()
    settings['PUMP_RUN_ID']=summary['run_id'];store=Spaces(settings)
    checkpoint=json.loads(store.read('checkpoint.json'))
    assert checkpoint==summary['state'], 'Checkpoint differs from completed local run'
    listing=store.client.list_objects_v2(Bucket=store.bucket,Prefix=store.key(''),MaxKeys=1000)
    assert not listing.get('IsTruncated'), 'Validation object listing exceeded bound'
    manifests=sorted(r['Key'] for r in listing.get('Contents',[]) if r['Key'].endswith('.manifest.json'))
    assert manifests, 'No durable manifests'
    totals=Counter();events=Counter();modes=Counter();coverage=Counter();raw_flags=Counter();seen=set()
    incomplete=set();resolved=set();unresolved=set();duplicate_ids=0;coverage_rows=[]
    for key in manifests:
        suffix=key.removeprefix(store.key(''))
        manifest=json.loads(store.read(suffix))
        for stream,metadata in manifest['files'].items():
            blob=store.read(metadata['key'].removeprefix(store.key('')))
            assert len(blob)==metadata['bytes'], 'Object size mismatch'
            assert hashlib.sha256(blob).hexdigest()==metadata['sha256'], 'Object hash mismatch'
            rows=[json.loads(line) for line in gzip.decompress(blob).splitlines()]
            assert len(rows)==manifest['rows'][stream], 'Row count mismatch'
            totals[stream+'_gzip_bytes']+=len(blob);totals[stream+'_rows']+=len(rows)
            for row in rows:
                if stream=='events':
                    events[row['event_name']]+=1;modes[row['mode_classification']]+=1
                    duplicate_ids+=row['event_id'] in seen;seen.add(row['event_id'])
                    assert row['mode_classification']!='mayhem', 'Known Mayhem decoded row retained'
                    assert row['fields'].get('mayhem_mode') is not True
                    assert row['fields'].get('is_mayhem_mode') is not True
                elif stream=='coverage':
                    coverage[row['kind']]+=1;coverage_rows.append(row)
                    if row['kind']=='incomplete_transaction_logs':incomplete.add(row['signature'])
                    if row['kind'] in ('enrichment_completed','enrichment_excluded_mayhem','confirmed_event_invalidated'):resolved.add(row['signature'])
                    if row['kind'] in ('enrichment_unresolved','enrichment_decode_failure'):unresolved.add(row['signature'])
                elif stream=='raw':
                    raw_flags[row['kind']]+=1
                    if row.get('contains_known_mayhem'):raw_flags['contains_known_mayhem']+=1
                    if row.get('mode_unknown'):raw_flags['mode_unknown']+=1
    assert duplicate_ids==0, 'Duplicate event IDs in validation run'
    assert coverage['capture_summary']==1, 'Final durable summary missing'
    duration=(datetime.fromisoformat(summary['last_stream_received_at_utc'])-
              datetime.fromisoformat(summary['first_stream_received_at_utc'])).total_seconds()
    compressed=sum(value for key,value in totals.items() if key.endswith('_gzip_bytes'))
    counters=summary['counters'];stream_bytes=counters['received_bytes']
    result=dict(run_id=summary['run_id'],bucket=store.bucket,prefix=store.key(''),
        readback_hash_and_row_checks='passed',manifest_count=len(manifests),totals=dict(totals),
        events=dict(events),mode_classifications=dict(modes),raw_flags=dict(raw_flags),
        coverage_counts=dict(coverage),duplicate_event_ids=duplicate_ids,
        incomplete_transactions=len(incomplete),resolved_incomplete_transactions=len(incomplete&resolved),
        unresolved_signatures=sorted((incomplete-resolved)|unresolved),
        stream_span_seconds=duration,received_bytes=stream_bytes,
        average_messages_per_second=counters['messages']/duration,
        peak_messages_per_second=counters['peak_messages_per_second'],
        average_direct_target_events_before_filter_per_second=counters['direct_target_events_before_filter']/duration,
        peak_direct_target_events_before_filter_per_second=counters['peak_direct_target_events_per_second'],
        retained_target_events=sum(events[name] for name in TARGETS),
        average_retained_target_events_per_second=sum(events[name] for name in TARGETS)/duration,
        compressed_dataset_GB_per_day=compressed/duration*86400/1e9,
        incoming_GB_per_day=stream_bytes/duration*86400/1e9,
        estimated_helius_credits_per_day=(stream_bytes/1e6*20+counters['selective_transaction_requests'])/duration*86400,
        counters=counters)
    output=path.with_name(path.name.replace('-summary.json','-verification.json'))
    output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('summary',type=Path)
    verify(parser.parse_args().summary)
