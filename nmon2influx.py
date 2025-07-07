#!/usr/bin/env python3
"""NMON to InfluxDB ingester and API proxy."""

import argparse
import glob
import os
from datetime import datetime
from typing import Dict, List, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import uvicorn

# ----------------------------------------------------------------------
# Generic NMON parser -> list of InfluxDB points
# ----------------------------------------------------------------------

def parse_nmon_file(path: str) -> Tuple[str, str, List[Dict]]:
    """Return (lpar, frame, points)."""
    zzzz: Dict[str, datetime] = {}
    headers: Dict[str, List[str]] = {}
    points: List[Dict] = []
    lpar = None
    frame = None

    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            key = parts[0]
            if key == 'AAA' and len(parts) > 2:
                if parts[1] == 'NodeName':
                    lpar = parts[2]
                elif parts[1] == 'SerialNumber':
                    frame = parts[2]
                continue
            if key == 'ZZZZ' and len(parts) > 3:
                tag = parts[1]
                try:
                    ts = datetime.strptime(f"{parts[3]} {parts[2]}", "%d-%b-%Y %H:%M:%S")
                    zzzz[tag] = ts
                except Exception:
                    pass
                continue
            if len(parts) > 1 and not parts[1].startswith('T'):
                headers[key] = parts[2:]
                continue
            if len(parts) > 1 and parts[1].startswith('T'):
                tag = parts[1]
                ts = zzzz.get(tag)
                if not ts:
                    continue
                measurement = key
                header = headers.get(measurement)
                if not header:
                    continue
                fields = {}
                for idx, col in enumerate(header):
                    if idx + 2 < len(parts):
                        try:
                            fields[col] = float(parts[idx+2])
                        except Exception:
                            fields[col] = None
                point = {
                    'measurement': measurement,
                    'tags': {'lpar': lpar or '', 'frame': frame or ''},
                    'fields': fields,
                    'time': ts
                }
                points.append(point)
                continue
    return lpar or '', frame or '', points

# ----------------------------------------------------------------------
# Ingestion
# ----------------------------------------------------------------------

def ingest_dir(input_dir: str, url: str, token: str, org: str, bucket: str):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    nmon_files = glob.glob(os.path.join(input_dir, '*.nmon'))
    if not nmon_files:
        print(f'No .nmon files found in {input_dir}')
        return
    for fp in nmon_files:
        lpar, frame, points = parse_nmon_file(fp)
        records = []
        for p in points:
            point = Point(p['measurement'])
            point.time(p['time'], WritePrecision.NS)
            for k, v in p['tags'].items():
                point.tag(k, v)
            for k, v in p['fields'].items():
                if v is not None:
                    point.field(k, v)
            records.append(point)
        if records:
            write_api.write(bucket=bucket, record=records)
            print(f'Ingested {len(records)} points from {os.path.basename(fp)}')
    client.close()

# ----------------------------------------------------------------------
# FastAPI proxy
# ----------------------------------------------------------------------
app = FastAPI()
client_holder = {'client': None}

@app.on_event('startup')
def startup_event():
    url = app.state.influx_url
    token = app.state.influx_token
    org = app.state.influx_org
    client_holder['client'] = InfluxDBClient(url=url, token=token, org=org)

@app.on_event('shutdown')
def shutdown_event():
    if client_holder['client']:
        client_holder['client'].close()

@app.get('/lpars')
def list_lpars(bucket: str):
    query = f"import \"influxdata/influxdb/schema\"\n schema.tagValues(bucket: \"{bucket}\", tag: \"lpar\")"
    result = client_holder['client'].query_api().query(org=app.state.influx_org, query=query)
    values = [r.get_value() for table in result for r in table.records]
    return JSONResponse(values)

@app.get('/frames')
def list_frames(bucket: str):
    query = f"import \"influxdata/influxdb/schema\"\n schema.tagValues(bucket: \"{bucket}\", tag: \"frame\")"
    result = client_holder['client'].query_api().query(org=app.state.influx_org, query=query)
    values = [r.get_value() for table in result for r in table.records]
    return JSONResponse(values)

@app.get('/query')
def run_query(q: str):
    try:
        result = client_holder['client'].query_api().query(org=app.state.influx_org, query=q)
        data = []
        for table in result:
            for record in table.records:
                data.append(record.values)
        return JSONResponse(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='NMON to InfluxDB tool')
    sub = parser.add_subparsers(dest='command')

    ing = sub.add_parser('ingest', help='Ingest NMON files to InfluxDB')
    ing.add_argument('--input-dir', required=True)
    ing.add_argument('--url', required=True)
    ing.add_argument('--token', required=True)
    ing.add_argument('--org', required=True)
    ing.add_argument('--bucket', required=True)

    api = sub.add_parser('api', help='Run FastAPI proxy server')
    api.add_argument('--host', default='0.0.0.0')
    api.add_argument('--port', type=int, default=8000)
    api.add_argument('--url', required=True)
    api.add_argument('--token', required=True)
    api.add_argument('--org', required=True)

    args = parser.parse_args()

    if args.command == 'ingest':
        ingest_dir(args.input_dir, args.url, args.token, args.org, args.bucket)
    elif args.command == 'api':
        app.state.influx_url = args.url
        app.state.influx_token = args.token
        app.state.influx_org = args.org
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
