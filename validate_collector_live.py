"""Bounded local test of the actual worker, writing only to the approved Space."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal

from collector import Spaces, Worker
from botocore.exceptions import ClientError
from ws_probe import config


async def main():
    settings=config()
    if (settings.get('SPACES_BUCKET'),settings.get('SPACES_REGION'))!=('fumppun','lon1'):
        raise ValueError('Use the owner-approved fumppun/lon1 bucket')
    now=datetime.now(timezone.utc)
    settings.update(PUMP_RUN_ID='validation-'+now.strftime('%Y%m%dT%H%M%SZ'),
        PUMP_STOP_AT_UTC=(now+timedelta(seconds=300)).isoformat(),
        PUMP_SPOOL_DIR=str(Path(__file__).parent/'data/collector-spool'),
        PUMP_MAX_RECEIVED_BYTES=str(256*1024*1024),PUMP_MAX_ENRICHMENT_REQUESTS='500',PUMP_EXCLUDE_MAYHEM='1')
    store=Spaces(settings)
    proof=json.dumps({'kind':'preflight','created_at_utc':now.isoformat()}).encode()
    await asyncio.to_thread(store.put,'preflight.json',proof)
    if await asyncio.to_thread(store.read,'preflight.json')!=proof:raise ValueError('Spaces readback mismatch')
    print(json.dumps({'kind':'spaces_preflight_passed','bucket':'fumppun','region':'lon1','run_id':settings['PUMP_RUN_ID']}),flush=True)
    worker=Worker(settings,store)
    async def terminate():
        await asyncio.sleep(300)
        os.kill(os.getpid(),signal.SIGTERM)
    timer=asyncio.create_task(terminate())
    await worker.run()
    timer.cancel()
    summary=dict(run_id=settings['PUMP_RUN_ID'],bucket='fumppun',region='lon1',
        first_stream_received_at_utc=worker.first_stream_received,last_stream_received_at_utc=worker.last_stream_received,
        counters=dict(worker.counters),state=worker.state,stop_signal_received=worker.stop.is_set(),
        local_remaining_chunks=len(list(worker.directory.glob('*.gz'))))
    path=Path(__file__).parent/'data'/f'{settings["PUMP_RUN_ID"]}-summary.json'
    path.write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)


if __name__=='__main__':
    try:asyncio.run(main())
    except Exception as exc:
        detail={'fatal_error_type':type(exc).__name__}
        if isinstance(exc,ClientError):
            detail.update(provider_error_code=exc.response.get('Error',{}).get('Code'),http_status=exc.response.get('ResponseMetadata',{}).get('HTTPStatusCode'))
        print(json.dumps(detail),flush=True)
        raise SystemExit(1)
