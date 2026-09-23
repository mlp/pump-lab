"""One bounded Pump research worker: logs, selective enrichment, gzip chunks in Spaces."""
import asyncio
import base64
from collections import Counter, OrderedDict
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import random
import signal
import time
import urllib.request
from urllib.parse import quote
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from websockets.asyncio.client import connect

from decode import CPI_TAG, IDL_SHA256, b58decode, decode_event
from ws_probe import PROGRAM, TARGETS, parse_logs, utc

MAX_CHUNK_BYTES = 4*1024*1024
MAX_MESSAGE_BYTES = 2*1024*1024
MAX_DEDUPE = 100_000
MAX_ENRICH_QUEUE = 128
COLLECTOR_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
REQUIRED_ENVIRONMENT = ('HELIUS','SPACES_ACCESS_KEY_ID','SPACES_SECRET_ACCESS_KEY',
                        'SPACES_BUCKET','SPACES_REGION','PUMP_RUN_ID')


def missing_environment(settings):
    return [key for key in REQUIRED_ENVIRONMENT if not settings.get(key,'').strip()]


def log(kind, **fields):
    print(json.dumps(dict(kind=kind, at_utc=utc(), **fields)), flush=True)


def event_identity(signature, encoded, occurrence):
    raw=base64.b64decode(encoded, validate=True)
    if raw.startswith(CPI_TAG):raw=raw[8:]
    return f'{signature}:{hashlib.sha256(raw).hexdigest()}:{occurrence}'


class Recent:
    def __init__(self, size=MAX_DEDUPE):
        self.size=size;self.values=OrderedDict()

    def add(self, identity):
        if identity in self.values:
            self.values.move_to_end(identity);return False
        self.values[identity]=None
        if len(self.values)>self.size:self.values.popitem(last=False)
        return True


def cpi_events(tx):
    message=tx['transaction']['message'];loaded=tx['meta'].get('loadedAddresses') or {}
    keys=message['accountKeys']+loaded.get('writable',[])+loaded.get('readonly',[])
    inners={r['index']:r['instructions'] for r in tx['meta'].get('innerInstructions') or []}
    events=[];failures=[]
    for outer,instruction in enumerate(message['instructions']):
        for inner,ix in [(None,instruction)]+list(enumerate(inners.get(outer,[]))):
            if keys[ix['programIdIndex']]!=PROGRAM:continue
            try:
                raw=b58decode(ix['data'])
                if not raw.startswith(CPI_TAG):continue
                raw=raw[8:];event=decode_event(raw)
                if event is None:raise ValueError('Unknown CPI event')
                name,fields=event
                events.append(dict(event_name=name,fields=fields,event_base64=base64.b64encode(raw).decode(),
                    event_sha256=hashlib.sha256(raw).hexdigest(),outer_instruction_index=outer,
                    inner_instruction_index=inner,event_ordinal=len(events)))
            except Exception as exc:
                failures.append(dict(outer_instruction_index=outer,inner_instruction_index=inner,error_type=type(exc).__name__))
    return events,failures


class Spaces:
    def __init__(self, settings):
        self.bucket=settings['SPACES_BUCKET'];self.prefix=settings.get('SPACES_PREFIX','pump-lab').strip('/')
        self.run_id=settings['PUMP_RUN_ID']
        if not self.run_id or '/' in self.run_id or not self.prefix:raise ValueError('Invalid dataset prefix or run ID')
        region=settings['SPACES_REGION']
        if not region.isalnum():raise ValueError('Invalid Spaces region')
        self.client=boto3.client('s3',endpoint_url=f'https://{region}.digitaloceanspaces.com',region_name=region,
            aws_access_key_id=settings['SPACES_ACCESS_KEY_ID'],aws_secret_access_key=settings['SPACES_SECRET_ACCESS_KEY'],
            config=Config(signature_version='s3v4',connect_timeout=5,read_timeout=15,
                retries={'max_attempts':2,'mode':'standard'},request_checksum_calculation='when_required',response_checksum_validation='when_required'))

    def key(self, suffix):return f'{self.prefix}/{self.run_id}/{suffix}'

    def read(self, suffix):
        try:return self.client.get_object(Bucket=self.bucket,Key=self.key(suffix))['Body'].read()
        except ClientError as exc:
            if exc.response['Error']['Code'] in ('NoSuchKey','404'):return None
            raise

    def put(self, suffix, body, content_type='application/json', compressed=False):
        args=dict(Bucket=self.bucket,Key=self.key(suffix),Body=body,ContentType=content_type,
                  ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode(),
                  Metadata={'sha256':hashlib.sha256(body).hexdigest(),'idl-sha256':IDL_SHA256})
        if compressed:args['ContentEncoding']='gzip'
        self.client.put_object(**args)


class Chunk:
    def __init__(self, directory, boot, sequence):
        self.stem=f'{boot}/{sequence:07d}';self.directory=directory
        self.files={};self.bytes=0;self.started=time.monotonic();self.started_at=utc();self.rows=Counter()
        self.last_received_at=None;self.last_slot=None

    def append(self, stream, row):
        if stream not in self.files:
            path=self.directory/f'{self.stem.replace("/","-")}.{stream}.jsonl.gz'
            self.files[stream]=(path,gzip.open(path,'wt',compresslevel=6))
        line=json.dumps(row,separators=(',',':'))+'\n'
        self.files[stream][1].write(line);self.bytes+=len(line.encode());self.rows[stream]+=1
        # A later enrichment response must never move the stream coverage boundary.
        if row.get('kind')=='logs_notification':
            self.last_received_at=row['received_at_utc'];self.last_slot=row['slot']

    def close(self):
        for _,file in self.files.values():file.close()
        return dict(stem=self.stem,started_at_utc=self.started_at,closed_at_utc=utc(),
            last_received_at_utc=self.last_received_at,last_slot=self.last_slot,
            uncompressed_bytes=self.bytes,rows=dict(self.rows),idl_sha256=IDL_SHA256,
            files={stream:dict(path=str(path),bytes=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
                   for stream,(path,_) in self.files.items()})


class Worker:
    def __init__(self, settings, store):
        self.settings=settings;self.store=store;self.boot=uuid.uuid4().hex
        self.directory=Path(settings.get('PUMP_SPOOL_DIR','/tmp/pump-spool'))/self.boot
        self.directory.mkdir(parents=True,exist_ok=False)
        self.stop=asyncio.Event();self.pending=asyncio.Queue(maxsize=2);self.enrich=asyncio.Queue(maxsize=MAX_ENRICH_QUEUE)
        self.rotation=asyncio.Lock();self.deferred=[]
        self.recent=Recent();self.transactions=Recent(20_000);self.sequence=0
        self.modes=OrderedDict();self.exclude_mayhem=settings.get('PUMP_EXCLUDE_MAYHEM','1')=='1'
        self.chunk=Chunk(self.directory,self.boot,self.sequence);self.counters=Counter();self.state={}
        deadline=settings.get('PUMP_STOP_AT_UTC')
        self.deadline=datetime.fromisoformat(deadline.replace('Z','+00:00')) if deadline else None
        if self.deadline is not None and self.deadline.tzinfo is None:raise ValueError('Stop time must include UTC offset')
        self.stream_limit=int(settings.get('PUMP_MAX_RECEIVED_BYTES','40000000000'))
        self.enrichment_limit=int(settings.get('PUMP_MAX_ENRICHMENT_REQUESTS','100000'))
        self.byte_base=0;self.request_base=0;self.attempt=0
        self.rate_tick=None;self.rate_messages=0;self.rate_events=0
        self.first_stream_received=None;self.last_stream_received=None
        self.outstanding_enrichment={}

    def measure_message(self, received, events=0):
        tick=received[:19]  # Fixed UTC one-second bins; constant memory.
        if tick!=self.rate_tick:self.rate_tick=tick;self.rate_messages=0;self.rate_events=0
        self.rate_messages+=1;self.rate_events+=events
        self.counters['peak_messages_per_second']=max(self.counters['peak_messages_per_second'],self.rate_messages)
        self.counters['peak_direct_target_events_per_second']=max(self.counters['peak_direct_target_events_per_second'],self.rate_events)

    def coverage(self, kind, **extra):
        self.chunk.append('coverage',dict(kind=kind,at_utc=utc(),boot_id=self.boot,**extra))
        log(kind,**extra)

    async def initialise(self):
        previous=await asyncio.to_thread(self.store.read,'checkpoint.json')
        if previous:
            self.state=json.loads(previous);self.byte_base=self.state.get('total_received_bytes',0)
            self.request_base=self.state.get('total_enrichment_requests',0)
            if self.deadline is not None and self.state['stop_at_utc']!=self.deadline.isoformat():raise ValueError('Run stop time cannot change on restart')
            self.deadline=datetime.fromisoformat(self.state['stop_at_utc'])
            if self.state['idl_sha256']!=IDL_SHA256:raise ValueError('Pinned IDL cannot change within a run')
            for suffix in self.state.get('recent_event_chunks',[]):
                raw=await asyncio.to_thread(self.store.read,suffix)
                if raw:
                    for line in gzip.decompress(raw).splitlines():
                        row=json.loads(line)
                        if row.get('event_id'):
                            self.recent.add(row['event_id']);self.observe_modes([row])
            self.coverage('restart_possible_gap',after_last_durable_utc=self.state.get('last_durable_received_at_utc'),
                          previous_checkpoint_utc=self.state.get('checkpoint_at_utc'),until_utc=utc())
        else:
            if self.deadline is None:self.deadline=datetime.now(timezone.utc)+timedelta(hours=24)
            remaining=(self.deadline-datetime.now(timezone.utc)).total_seconds()
            if not 0<remaining<=86400:raise ValueError('Initial run must end within 24 hours')
            self.state=dict(run_id=self.settings['PUMP_RUN_ID'],started_at_utc=utc(),stop_at_utc=self.deadline.isoformat(),
                idl_sha256=IDL_SHA256,total_received_bytes=0,recent_event_chunks=[],last_durable_received_at_utc=None)
            await asyncio.to_thread(self.store.put,'checkpoint.json',json.dumps(self.state).encode())
        self.coverage('worker_started',run_id=self.settings['PUMP_RUN_ID'],stop_at_utc=self.deadline.isoformat(),
                      collector_sha256=COLLECTOR_SHA256,
                      exclude_known_mayhem=self.exclude_mayhem,
                      source_commitment='confirmed',dedupe='signature:event_payload_sha256:occurrence; bounded recent cache plus durable last-two-chunk restoration')

    def observe_modes(self, events):
        for event in events:
            fields=event['fields'];mint=fields.get('mint')
            mode=fields.get('is_mayhem_mode') if event['event_name']=='CreateEvent' else fields.get('mayhem_mode') if event['event_name']=='TradeEvent' else None
            if mint and mode is not None:
                self.modes[mint]=mode;self.modes.move_to_end(mint)
                if len(self.modes)>20_000:self.modes.popitem(last=False)

    def mode(self,event):
        return self.modes.get(event['fields'].get('mint'))

    def mayhem_only(self,events):
        market=[event for event in events if event['event_name'] in TARGETS]
        return self.exclude_mayhem and bool(market) and all(self.mode(event) is True for event in market)

    def event_rows(self, signature, slot, received, events, source):
        self.observe_modes(events)
        occurrences=Counter()
        for event in events:
            digest=event['event_sha256'];ordinal=occurrences[digest];occurrences[digest]+=1
            identity=event_identity(signature,event['event_base64'],ordinal)
            if not self.recent.add(identity):self.counters['duplicate_events']+=1;continue
            mode=self.mode(event)
            if self.exclude_mayhem and mode is True:
                self.counters['excluded_mayhem_events']+=1;continue
            # Retain all decoded Pump event families; no derived trading metrics.
            self.chunk.append('events',dict(event_id=identity,signature=signature,slot=slot,received_at_utc=received,
                source=source,mode_classification='unknown' if mode is None else 'mayhem' if mode else 'non_mayhem',
                commitment='finalized' if source=='selective_transaction' else 'confirmed',idl_sha256=IDL_SHA256,**event))
            # Missing mode is explicit: no launch lookup or ownership inference.
            # The source fields retain the original bool for Create/Trade events.
            self.counters[event['event_name']]+=1

    def notification(self, message, received):
        value=message['params']['result']['value'];slot=message['params']['result']['context']['slot']
        signature=value['signature']
        self.first_stream_received=self.first_stream_received or received;self.last_stream_received=received
        # Include intentionally excluded/failed notifications in coverage, never enrichment time.
        self.chunk.last_received_at=received;self.chunk.last_slot=slot
        if value['err'] is not None:
            self.measure_message(received);self.counters['failed_transaction_notifications']+=1;return
        self.counters['successful_transaction_notifications']+=1
        if not self.transactions.add(signature):
            self.measure_message(received);self.counters['duplicate_notifications']+=1;return
        events,failures,stats=parse_logs(value['logs'])
        targets=sum(event['event_name'] in TARGETS for event in events)
        self.counters['direct_target_events_before_filter']+=targets;self.measure_message(received,targets)
        self.observe_modes(events)
        self.counters.update(stats)
        if not events:self.counters['successful_without_decoded_event']+=1
        self.counters['decode_failures']+=len(failures)
        migration=any(line in ('Program log: Instruction: Migrate','Program log: Instruction: MigrateV2') for line in value['logs'])
        needs_enrichment=bool(stats.get('truncated_logs') or stats.get('unmatched_return') or failures or
                              (migration and not any(e['event_name']=='CompletePumpAmmMigrationEvent' for e in events)))
        if self.mayhem_only(events) and not needs_enrichment:
            self.counters['excluded_mayhem_transactions']+=1
            self.counters['excluded_mayhem_events']+=len(events)
            return
        self.chunk.append('raw',dict(kind='logs_notification',received_at_utc=received,slot=slot,signature=signature,
            commitment='confirmed',contains_known_mayhem=any(self.mode(e) is True for e in events),
            mode_unknown=any(self.mode(e) is None for e in events if e['event_name'] in TARGETS),
            incomplete_logs=needs_enrichment,message=message))
        self.event_rows(signature,slot,received,events,'direct_logs')
        if failures:self.coverage('decode_failure',signature=signature,slot=slot,failures=failures)
        if needs_enrichment:
            self.coverage('incomplete_transaction_logs',signature=signature,slot=slot,stats=stats)
            try:
                job=dict(signature=signature,slot=slot,original_received_at_utc=received)
                self.enrich.put_nowait(job);self.outstanding_enrichment[signature]=job
            except asyncio.QueueFull:self.coverage('enrichment_unresolved',signature=signature,reason='bounded_queue_full')

    async def rotate(self, force=False):
        async with self.rotation:
            if not self.chunk.files:return
            if not force and self.chunk.bytes<MAX_CHUNK_BYTES and time.monotonic()-self.chunk.started<30:return
            item=self.chunk.close();item['total_received_bytes']=self.byte_base+self.counters['received_bytes']
            item['total_enrichment_requests']=self.request_base+self.counters['selective_transaction_requests']
            self.sequence+=1;self.chunk=Chunk(self.directory,self.boot,self.sequence)
            while self.pending.full() and not self.stop.is_set() and not self.expired():await asyncio.sleep(.25)
            if self.deferred or self.pending.full():self.deferred.append(item)
            else:self.pending.put_nowait(item)

    async def upload_loop(self):
        while True:
            item=await self.pending.get()
            try:
                if item is None:return
                delay=1
                while True:
                    try:
                        public=dict(item,files={})
                        event_suffix=None
                        for stream,metadata in item['files'].items():
                            path=Path(metadata['path']);suffix=f'{item["stem"]}.{stream}.jsonl.gz'
                            await asyncio.to_thread(self.store.put,suffix,path.read_bytes(),'application/x-ndjson',True)
                            public['files'][stream]={k:v for k,v in metadata.items() if k!='path'}
                            public['files'][stream]['key']=self.store.key(suffix)
                            if stream=='events':event_suffix=suffix
                        await asyncio.to_thread(self.store.put,f'{item["stem"]}.manifest.json',json.dumps(public).encode())
                        state=dict(self.state,checkpoint_at_utc=utc(),last_chunk_manifest=f'{item["stem"]}.manifest.json',
                            total_received_bytes=item['total_received_bytes'],total_enrichment_requests=item['total_enrichment_requests'])
                        if item['last_received_at_utc']:state['last_durable_received_at_utc']=item['last_received_at_utc']
                        if event_suffix:state['recent_event_chunks']=(state.get('recent_event_chunks',[])+[event_suffix])[-2:]
                        await asyncio.to_thread(self.store.put,'checkpoint.json',json.dumps(state).encode())
                        self.state=state
                        for metadata in item['files'].values():Path(metadata['path']).unlink()
                        log('chunk_durable',manifest=f'{item["stem"]}.manifest.json',rows=item['rows']);break
                    except Exception as exc:
                        # Keep the same local files and object keys until acknowledgement.
                        log('upload_failed',error_type=type(exc).__name__,retry_seconds=delay)
                        await asyncio.sleep(delay);delay=min(delay*2,30)
            finally:self.pending.task_done()

    def fetch_transaction(self, signature):
        endpoint='https://mainnet.helius-rpc.com/?api-key='+quote(self.settings['HELIUS'],safe='')
        payload=dict(jsonrpc='2.0',id=1,method='getTransaction',params=[signature,
            dict(encoding='json',commitment='finalized',maxSupportedTransactionVersion=1)])
        request=urllib.request.Request(endpoint,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=15) as response:
            body=response.read(2*1024*1024+1)
        if len(body)>2*1024*1024:raise ValueError('Enrichment response too large')
        return json.loads(body),len(body)

    async def enrich_loop(self):
        while True:
            job=await self.enrich.get()
            try:
                if job is None:return
                # Never fetch every trade: only explicit incomplete-log evidence.
                age=(datetime.now(timezone.utc)-datetime.fromisoformat(job['original_received_at_utc'])).total_seconds()
                await asyncio.sleep(max(1,30-age))
                result=None
                for attempt in range(2):
                    if self.request_base+self.counters['selective_transaction_requests']>=self.enrichment_limit:
                        self.coverage('enrichment_unresolved',signature=job['signature'],reason='request_budget_reached');break
                    try:
                        self.counters['selective_transaction_requests']+=1
                        response,response_bytes=await asyncio.to_thread(self.fetch_transaction,job['signature'])
                        self.counters['enrichment_received_bytes']+=response_bytes
                        result=response.get('result')
                        if response.get('error'):
                            self.coverage('enrichment_provider_error',signature=job['signature'],error_code=response['error'].get('code'))
                        if result:break
                    except Exception as exc:
                        self.coverage('enrichment_error',signature=job['signature'],error_type=type(exc).__name__)
                    if attempt==0:await asyncio.sleep(15)
                if not result:
                    self.coverage('enrichment_unresolved',signature=job['signature'],reason='two_attempt_limit');continue
                if result['meta']['err'] is not None:
                    self.coverage('confirmed_event_invalidated',signature=job['signature'],reason='finalized_transaction_failed');continue
                events,failures=cpi_events(result)
                self.observe_modes(events)
                if self.mayhem_only(events) and not failures:
                    self.counters['excluded_mayhem_enrichments']+=1
                    self.coverage('enrichment_excluded_mayhem',signature=job['signature']);continue
                self.chunk.append('raw',dict(kind='selective_transaction',received_at_utc=utc(),**job,
                    contains_known_mayhem=any(self.mode(e) is True for e in events),
                    mode_unknown=any(self.mode(e) is None for e in events if e['event_name'] in TARGETS),transaction=result))
                self.event_rows(job['signature'],result['slot'],utc(),events,'selective_transaction')
                self.coverage('enrichment_completed' if not failures else 'enrichment_decode_failure',
                    signature=job['signature'],decoded_events=len(events),failures=failures)
                await self.rotate()
            except Exception as exc:
                self.coverage('enrichment_unresolved',signature=job['signature'],reason='processing_error',error_type=type(exc).__name__)
            finally:
                if job is not None:self.outstanding_enrichment.pop(job['signature'],None)
                self.enrich.task_done()

    def expired(self):return self.deadline is not None and datetime.now(timezone.utc)>=self.deadline

    async def run(self):
        for sig in (signal.SIGINT,signal.SIGTERM):asyncio.get_running_loop().add_signal_handler(sig,self.stop.set)
        await self.initialise()
        uploader=asyncio.create_task(self.upload_loop())
        enrichers=[asyncio.create_task(self.enrich_loop()) for _ in range(2)]
        endpoint=self.settings.get('PUMP_WSS_URL') or 'wss://mainnet.helius-rpc.com/?api-key='+quote(self.settings['HELIUS'],safe='')
        if not endpoint.startswith('wss://'):raise ValueError('TLS WSS required')
        backoff=1;last_received=None;gap_start=utc()
        try:
            while not self.stop.is_set() and not self.expired():
                if self.byte_base+self.counters['received_bytes']>=self.stream_limit:
                    self.coverage('stream_byte_budget_reached',limit=self.stream_limit);break
                # When storage stalls, stop receiving instead of growing disk unbounded.
                while self.pending.full() and not self.stop.is_set() and not self.expired():await asyncio.sleep(1)
                if self.stop.is_set() or self.expired():break
                self.attempt+=1;connected_at=None
                try:
                    async with connect(endpoint,open_timeout=15,close_timeout=3,ping_interval=20,ping_timeout=20,
                                       max_size=MAX_MESSAGE_BYTES,max_queue=16) as ws:
                        await ws.send(json.dumps(dict(jsonrpc='2.0',id=1,method='logsSubscribe',
                            params=[{'mentions':[PROGRAM]},{'commitment':'confirmed'}])))
                        ack=json.loads(await asyncio.wait_for(ws.recv(),15))
                        if not isinstance(ack.get('result'),int):raise ValueError('Subscription rejected')
                        connected_at=time.monotonic()
                        self.coverage('subscribed',attempt=self.attempt,possible_gap_start_utc=gap_start,possible_gap_end_utc=utc())
                        next_heartbeat=time.monotonic()+30
                        while not self.stop.is_set() and not self.expired():
                            try:wire=await asyncio.wait_for(ws.recv(),1)
                            except asyncio.TimeoutError:
                                await self.rotate()
                                if time.monotonic()>=next_heartbeat:
                                    self.coverage('heartbeat',counters=dict(self.counters),last_received_at_utc=last_received)
                                    next_heartbeat=time.monotonic()+30
                                continue
                            received=utc();last_received=received
                            self.counters['received_bytes']+=len(wire.encode() if isinstance(wire,str) else wire)
                            self.counters['messages']+=1
                            message=json.loads(wire)
                            if message.get('method')=='logsNotification':self.notification(message,received)
                            else:self.coverage('unexpected_message')
                            if self.pending.full():
                                self.coverage('storage_backpressure_possible_gap',after_utc=last_received);break
                            await self.rotate()
                            if self.byte_base+self.counters['received_bytes']>=self.stream_limit:break
                            if time.monotonic()>=next_heartbeat:
                                self.coverage('heartbeat',counters=dict(self.counters),last_received_at_utc=last_received)
                                next_heartbeat=time.monotonic()+30
                        if connected_at and time.monotonic()-connected_at>60:backoff=1
                except Exception as exc:
                    self.coverage('connection_error',error_type=type(exc).__name__,attempt=self.attempt)
                gap_start=last_received or gap_start
                self.coverage('disconnected',last_received_at_utc=last_received,possible_gap_start_utc=gap_start)
                await self.rotate(force=True)
                if not self.stop.is_set() and not self.expired():
                    try:await asyncio.wait_for(self.stop.wait(),backoff+random.random())
                    except asyncio.TimeoutError:pass
                    backoff=min(backoff*2,30)
        finally:
            # A restart can recover the durable boundary, not RAM or temporary disk.
            self.coverage('capture_stopped',expired=self.expired(),pending_enrichment=self.enrich.qsize(),counters=dict(self.counters))
            try:
                await asyncio.wait_for(self.enrich.join(),45)
            except asyncio.TimeoutError:
                self.coverage('enrichment_unresolved_at_stop',queued=self.enrich.qsize(),
                    transactions=list(self.outstanding_enrichment.values()))
            for task in enrichers:task.cancel()
            await asyncio.gather(*enrichers,return_exceptions=True)
            self.coverage('capture_summary',counters=dict(self.counters),
                first_stream_received_at_utc=self.first_stream_received,last_stream_received_at_utc=self.last_stream_received)
            async def drain_uploads():
                await self.rotate(force=True)
                for item in self.deferred:await self.pending.put(item)
                self.deferred.clear()
                await self.pending.join()
            try:await asyncio.wait_for(drain_uploads(),75)
            except asyncio.TimeoutError:log('shutdown_upload_incomplete',last_durable_received_at_utc=self.state.get('last_durable_received_at_utc'))
            uploader.cancel()
            try:await uploader
            except asyncio.CancelledError:pass
        # App Platform restarts exited workers. Stay idle after the fixed run deadline.
        if not self.stop.is_set():
            log('run_finished_idle',stop_at_utc=self.deadline.isoformat())
            await self.stop.wait()


if __name__=='__main__':
    logging.getLogger('websockets').setLevel(logging.CRITICAL)
    settings=dict(os.environ)
    missing=missing_environment(settings)
    if missing:
        log('fatal',error_type='MissingEnvironment',missing_environment_variables=missing)
        raise SystemExit(1)
    try:
        asyncio.run(Worker(settings,Spaces(settings)).run())
    except Exception as exc:
        detail={}
        if isinstance(exc,KeyError) and exc.args and exc.args[0] in REQUIRED_ENVIRONMENT+('stop_at_utc','idl_sha256'):
            detail['missing_key']=exc.args[0]
        log('fatal',error_type=type(exc).__name__,**detail)
        raise SystemExit(1)
