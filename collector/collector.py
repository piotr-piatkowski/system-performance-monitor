#!/usr/bin/python3

import os
import sys
import logging
import subprocess
import argparse
from influxdb import InfluxDBClient

SECTOR_SIZE = 512

logger = logging.getLogger("collector")

def parse_data_line(label, values):
    data_row = {}
    tags = {}
    if label == "CPU":
        tps = int(values[0])
        tick_keys = ('sys', 'user', 'nice', 'idle', 'wait', 'irq', 'softirq',
                    'steal', 'guest')
        keys = ('tps', 'ncpu', *tick_keys, 'freq', 'freq_perc',
                'instructions', 'cycles')
        for k, v in zip(keys, values):
            v = int(v)
            if k in tick_keys:
                v = v / tps
            data_row[f"cpu.{k}"] = v
        data_row['cpu.usage'] = 1.0 - data_row['cpu.idle'] / data_row['cpu.ncpu']
    elif label == "CPL":
        keys = ('ncpu', 'load1', 'load5', 'load15', 'ctxsw', 'hwirqs')
        for k, v in zip(keys, values):
            if k.startswith('load'):
                v = float(v)
            else:
                v = int(v)
            data_row[f"load.{k}"] = v
    elif label == "MEM":
        page_keys = ()
        huge_page_keys = ('htotal', 'hfree')
        keys = ('page_size', 'total', 'free', 'cache', 'buff', 'slab', 'dirty',
                'recl_slab', 'baloon', 'shared', 'res_shared', 'swapped',
                'huge_page_size', *huge_page_keys, 'zfs_cache', 'ksm',
                'ksm2')
        for k, v in zip(keys, values):
            v = int(v)
            if k == 'page_size':
                page_size = v
                continue
            elif k == 'huge_page_size':
                huge_page_size = v
                continue
            elif k in huge_page_keys:
                v *= huge_page_size
            else:
                v *= page_size
            data_row[f"mem.{k}"] = v
        free_mem = sum(data_row[f'mem.{k}'] for k in
                       ('free', 'cache', 'buff', 'zfs_cache'))
        data_row[f'mem.usage'] = 1.0 - free_mem / data_row['total']
    elif label == 'DSK':
        dsk_label = values.pop(0)
        tags['disk'] = dsk_label
        keys = ('io_ms', 'read_op', 'read_sect', 'write_op', 'write_sect',
                'discard_op', 'discard_sect')
        for k, v in zip(keys, values):
            v = int(v)
            data_row[f"dsk.{k}"] = v
        data_row[f"dsk.read_bytes"] = data_row["dsk.read_sect"] * SECTOR_SIZE
        data_row[f"dsk.write_bytes"] = data_row["dsk.write_sect"] * SECTOR_SIZE
        data_row['dsk.usage'] = data_row['io_ms'] / 1000

    return data_row, tags

def collect_data(args):

    influx_conn = InfluxDBClient(args.influx_host, args.influx_port, '', '',
                                 args.influx_database)
    influx_conn.create_database(args.influx_database)
    proc = subprocess.Popen("atop -P CPU,CPL,MEM,DSK 1", shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL, text=True)

    points = []
    while proc.returncode is None:
        line = proc.stdout.readline().rstrip('\n')
        logger.debug(f"{line=}")
        if line is None:
            logger.warning("EOF reading from stdout")
            break
        label, *values = line.split()
        logger.debug(f"{label=} {values=}")
        if label == 'RESET':
            pass
        elif label == 'SEP':
            # flush data
            influx_conn.write_points(points)
            points = []
        else:
            host, ts, date, time, delta, *values = values
            ts = int(ts)
            delta = int(delta)
            if delta > 1:
                # ignore initial values
                continue

            fields, tags = parse_data_line(label, values)
            points.append({
                'measurement': 'performance',
                'tags': tags,
                'fields': fields,
            })
            logger.debug(f"{fields=}")


def main():
    parser = argparse.ArgumentParser(
        description='Collect data from atop and put into influxdb',
    )

    parser.set_defaults(loglevel=logging.INFO)
    parser.add_argument(
        '-d', '--debug', dest='loglevel',
        action="store_const", const=logging.DEBUG,
        help='Enable debug mode'
    )

    parser.add_argument(
        '-H', '--influx-host', default='localhost',
        help="Hostname of influxdb server"
    )

    parser.add_argument(
        '-P', '--influx-port', default=8086, type=int,
        help="Port of influxdb server"
    )

    parser.add_argument(
        '-D', '--influx-database', default='performance',
        help="Influx database name"
    )

    args = parser.parse_args()

    log_format = '%(asctime)s %(name)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=args.loglevel,
        stream=sys.stderr,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    collect_data(args)

main()
