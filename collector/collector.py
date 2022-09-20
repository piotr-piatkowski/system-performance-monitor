#!/usr/bin/python3

import os
import sys
import logging
import subprocess
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("collector")

conn = InfluxDBClient('localhost', 8086, '', '', 'performance')
proc = subprocess.Popen("atop -P CPU,CPL,MEM,DSK 1", shell=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        stdin=subprocess.DEVNULL, text=True)

logger.info(f"Started proc, {proc=}, {proc.returncode=}")
while proc.returncode is None:
    line = proc.stdout.readline().rstrip('\n')
    if line is None:
        logger.warning("EOF reading from stdout")
        break
    label, *values = line.split()
    logger.debug(f"{label=} {values=}")
    if label in ('RESET', 'SEP'):
        continue
    host, ts, date, time, delta, *values = values
    ts = int(ts)
    delta = int(delta)
    if delta > 1:
        # ignore initial values
        continue
    data_row = {}
    if label == "CPU":
        tps = int(values[0])
        tick_keys = ('sys', 'user', 'nice', 'idle', 'wait', 'irq', 'softirq',
                     'steal', 'guest')
        keys = ('tps', 'ncpu', *tick_keys, 'freq', 'freq_perc',
                'instructions', 'cycles')
        for i, k in enumerate(keys):
            v = int(values[i])
            if k in tick_keys:
                v = v / tps
            data_row[f"cpu.{k}"] = v
    elif label == "CPL":
        keys = ('ncpu', 'load1', 'load5', 'load15', 'ctxsw', 'hwirqs')
        for i, k in enumerate(keys):
            if k.startswith('load'):
                v = float(values[i])
            else:
                v = int(values[i])
            data_row[f"load.{k}"] = v
    elif label == "MEM":
        page_keys = ()
        huge_page_keys = ('htotal', 'hfree')
        keys = ('page_size', 'total', 'free', 'cache', 'buff', 'slab', 'dirty',
                'recl_slab', 'baloon', 'shared', 'res_shared', 'swapped',
                'huge_page_size', *huge_page_keys, 'zfs_cache', 'ksm',
                'ksm2')
        for i, k in enumerate(keys):
            if k == 'page_size':
                page_size = int(values[i])
                continue
            elif k == 'huge_page_size':
                huge_page_size = int(values[i])
                continue
            elif k in huge_page_keys:
                v = int(values[i]) * huge_page_size
            else:
                v = int(values[i]) * page_size
            data_row[f"mem.{k}"] = v
    elif label == 'DSK':
        dsk_label = values.pop(0)
        keys = ('io_ms', 'read_op', 'read_sect', 'write_op', 'write_sect',
                'discard_op', 'discard_sect')
        for i, k in enumerate(keys):
            v = int(values[i])
            data_row[f"dsk.{dsk_label}.{k}"] = v

    logger.info(f"{data_row=}")
