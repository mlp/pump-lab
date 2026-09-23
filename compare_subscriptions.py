"""Five-minute, simultaneous byte/coverage comparison; no transaction RPC fetches."""
import asyncio
import base64
from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
import logging
from pathlib import Path
import time
from urllib.parse import quote

from websockets.asyncio.client import connect
from decode import CPI_TAG, b58decode, decode_event
from ws_probe import PROGRAM, TARGETS, config, parse_logs, utc

DURATION=300
BYTE_LIMIT=768*1024*1024


def parsed_cpi_events(tx):
    instructions=list(tx['transaction']['message']['instructions'])
    for group in tx['meta'].get('innerInstructions') or []:instructions.extend(group['instructions'])
    events=[];failures=[]
    for ix in instructions:
        if ix.get('programId')!=PROGRAM or not ix.get('data'):continue
        try:
            raw=b58decode(ix['data'])
            if not raw.startswith(CPI_TAG):continue
            decoded=decode_event(raw)
            if decoded is None:raise ValueError('Unknown CPI discriminator')
            name,fields=decoded
            events.append(dict(event_name=name,fields=fields,event_sha256=hashlib.sha256(raw[8:]).hexdigest()))
        except Exception as exc:failures.append(type(exc).__name__)
    return events,failures


async def capture():
    output=Path('data')/('subscription-comparison-'+datetime.now().strftime('%Y%m%dT%H%M%S'))
    output.mkdir(exist_ok=False)
    settings=config();endpoint=settings.get('PUMP_WSS_URL') or 'wss://mainnet.helius-rpc.com/?api-key='+quote(settings['HELIUS'],safe='')
    requests=[dict(jsonrpc='2.0',id=1,method='logsSubscribe',params=[{'mentions':[PROGRAM]},{'commitment':'confirmed'}]),
        dict(jsonrpc='2.0',id=2,method='transactionSubscribe',params=[{'failed':False,'accountInclude':[PROGRAM]},
            {'commitment':'confirmed','encoding':'jsonParsed','transactionDetails':'full','showRewards':False,'maxSupportedTransactionVersion':1}])]
    (output/'requests.json').write_text(json.dumps(requests,indent=2)+'\n')
    counts={name:Counter() for name in ('logs','full_success')};subscriptions={};statuses=[];begin=None;start_utc=None
    def status(kind,**fields):
        record=dict(kind=kind,at_utc=utc(),**fields);statuses.append(record)
        print(json.dumps(record),flush=True)
    try:
        async with connect(endpoint,open_timeout=15,close_timeout=3,ping_interval=20,ping_timeout=20,
                           max_size=8*1024*1024,max_queue=32) as ws:
            for request in requests:await ws.send(json.dumps(request))
            with gzip.open(output/'messages.jsonl.gz','wt',compresslevel=3) as raw:
                next_progress=time.monotonic()+30;ack_deadline=time.monotonic()+20
                while begin is None or time.monotonic()-begin<DURATION:
                    if begin is None and time.monotonic()>ack_deadline:raise TimeoutError('Subscription ACK timeout')
                    try:wire=await asyncio.wait_for(ws.recv(),1)
                    except asyncio.TimeoutError:continue
                    message=json.loads(wire)
                    if message.get('id') in (1,2):
                        if not isinstance(message.get('result'),int):
                            status('subscription_rejected',request_id=message['id'],code=message.get('error',{}).get('code'));break
                        name='logs' if message['id']==1 else 'full_success';subscriptions[message['result']]=name
                        status('subscribed',stream=name)
                        if len(subscriptions)==2:begin=time.monotonic();start_utc=utc();status('common_capture_started')
                        continue
                    if begin is None:continue
                    stream=subscriptions.get(message.get('params',{}).get('subscription'))
                    if stream is None:status('unexpected_message');continue
                    size=len(wire.encode() if isinstance(wire,str) else wire)
                    counts[stream]['messages']+=1;counts[stream]['received_bytes']+=size
                    raw.write(json.dumps(dict(stream=stream,wire_bytes=size,received_at_utc=utc(),message=message),separators=(',',':'))+'\n')
                    if sum(c['received_bytes'] for c in counts.values())>=BYTE_LIMIT:status('byte_limit_reached');break
                    if time.monotonic()>=next_progress:
                        status('progress',streams={k:dict(v) for k,v in counts.items()});raw.flush();next_progress=time.monotonic()+30
                elapsed=time.monotonic()-begin if begin else 0;end_utc=utc()
                status('capture_finished',seconds=elapsed,streams={k:dict(v) for k,v in counts.items()})
    except Exception as exc:
        status('connection_error',error_type=type(exc).__name__)
        elapsed=time.monotonic()-begin if begin else 0;end_utc=utc()
    summary=dict(directory=str(output),started_at_utc=start_utc,ended_at_utc=end_utc,seconds=elapsed,
                 streams={k:dict(v) for k,v in counts.items()},coverage=statuses,transaction_rpc_calls=0)
    (output/'capture.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'output_directory':str(output)}),flush=True)
    if elapsed>0:analyse(output)


def analyse(output):
    summary=json.loads((output/'capture.json').read_text());seconds=summary['seconds']
    stats={name:Counter() for name in ('logs','full_success')};txs={name:{} for name in stats}
    with gzip.open(output/'messages.jsonl.gz','rt') as raw:
        for line in raw:
            row=json.loads(line);name=row['stream'];result=row['message']['params']['result'];stat=stats[name]
            if name=='logs':
                value=result['value'];slot=result['context']['slot'];signature=value['signature'];err=value['err'];logs=value['logs']
            else:
                value=result['transaction'];slot=result['slot'];signature=result['signature'];err=value['meta']['err'];logs=value['meta'].get('logMessages') or []
            outcome='failed' if err is not None else 'successful';stat[outcome+'_bytes']+=row['wire_bytes'];stat[outcome+'_messages']+=1
            if err is not None:continue
            direct,failures,log_stats=parse_logs(logs)
            if name=='full_success':events,cpi_failures=parsed_cpi_events(value);stat['cpi_decode_failures']+=len(cpi_failures)
            else:events=direct
            stat['log_decode_failures']+=len(failures)
            needs_recovery=bool(log_stats.get('truncated_logs') or log_stats.get('unmatched_return') or failures or
                (any(line in ('Program log: Instruction: Migrate','Program log: Instruction: MigrateV2') for line in logs) and
                 not any(e['event_name']=='CompletePumpAmmMigrationEvent' for e in direct)))
            stat['needs_selective_recovery']+=needs_recovery
            if signature in txs[name]:stat['duplicate_notifications']+=1;continue
            targets=Counter((e['event_name'],e['event_sha256']) for e in events if e['event_name'] in TARGETS)
            txs[name][signature]=dict(slot=slot,targets=targets,incomplete_logs=needs_recovery)
            stat.update(e['event_name'] for e in events if e['event_name'] in TARGETS)
    if any(not txs[name] for name in txs):return
    lo=max(min(r['slot'] for r in txs[name].values()) for name in txs)+2
    hi=min(max(r['slot'] for r in txs[name].values()) for name in txs)-2
    scoped={name:{sig:r for sig,r in rows.items() if lo<=r['slot']<=hi} for name,rows in txs.items()}
    direct,full=scoped['logs'],scoped['full_success'];common=set(direct)&set(full)
    comparison=Counter();differences=[]
    for sig in common:
        left,right=direct[sig]['targets'],full[sig]['targets'];missing=right-left;extra=left-right
        comparison['exact_target_events_matched']+=sum((left&right).values())
        comparison['full_only_target_events']+=sum(missing.values());comparison['logs_only_target_events']+=sum(extra.values())
        if missing or extra:differences.append(dict(signature=sig,slot=direct[sig]['slot'],logs_incomplete=direct[sig]['incomplete_logs'],
            full_only=[dict(event_name=k[0],event_sha256=k[1],count=v) for k,v in missing.items()],
            logs_only=[dict(event_name=k[0],event_sha256=k[1],count=v) for k,v in extra.items()]))
    rates={name:dict(stream_credits_per_day=summary['streams'][name]['received_bytes']/seconds*86400/1e6*20,
                    received_bytes=summary['streams'][name]['received_bytes'],**stat) for name,stat in stats.items()}
    rates['logs']['estimated_total_credits_per_day_with_one_rpc_per_recovery']=rates['logs']['stream_credits_per_day']+stats['logs']['needs_selective_recovery']/seconds*86400
    result=dict(capture=summary,rates=rates,interior_slot_range=[lo,hi],common_successful_signatures=len(common),
        logs_only_signatures=sorted(set(direct)-set(full)),full_only_signatures=sorted(set(full)-set(direct)),
        event_comparison=dict(comparison),difference_transactions=differences,
        full_stream_to_logs_stream_byte_ratio=summary['streams']['full_success']['received_bytes']/summary['streams']['logs']['received_bytes'],
        note='Same connection, two concurrent subscriptions. Interior slots trim two slots at each boundary; no provider is presumed complete. Credits estimated at 20 per decimal MB, not account billing telemetry.')
    (output/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('capture','difference_transactions','logs_only_signatures','full_only_signatures')},indent=2),flush=True)


if __name__=='__main__':
    logging.getLogger('websockets').setLevel(logging.CRITICAL)
    asyncio.run(capture())
