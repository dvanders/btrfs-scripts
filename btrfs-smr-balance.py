#!/usr/bin/env python3

# The goal of this is to gradually balance a btrfs filesystem which contains DM-SMR drives.
# Such drive are described in detail at https://www.usenix.org/node/188434

# DM-SMR drives have a small PMR cache for random writes -- we want to balance the
# filesystem without overflowing that cache, because that would lead to extremely slow
# drive responsiveness.

# We assume that a DM-SMR drive in good shape could easily balance a 1GB chunk in under
# a minute. We don't know a priori if the chunk has been cached or written directly to an
# SMR block. In the case it was cached, it would take roughly 100s to clean after some
# unknown idle period.

# Given the above, here is our balancing heuristic:
# * balance one chunk from the drive with the amount of unallocated space
# * if it took longer than 60s, increase a per-chunk sleep interval with some exp backoff
# * otherwise, decrease the per-chunk sleep interval
# * stop after the stdev of the unallocated space on all drives drops below 5GB

# Here are some config options, to be set for your environment:

FILESYSTEM = '/media/btrfs' # your filesystem to be balanced
CHUNK_TIMEOUT = 60 # time in seconds to decide if a single balance operation was fast enough
MAX_SLEEP = 7200 # max time in seconds to sleep between balance operations
STDEV_LIMIT = 5*1024*1024*1024 # standard deviation in unallocated bytes after which to exit

# ------------------

from functools import lru_cache
import logging
import psutil
import statistics
import subprocess
import sys
import time

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

@lru_cache
def fib(n):
    if n < 2:
        return 1
    return fib(n-2) + fib(n-1)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%4.2f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.2f%s%s" % (num, 'Yi', suffix)

def bal_chunk():
    least_empty_dev_id = None
    least_empty_dev_path = None
    least_empty_dev_unallocated = float('inf')

    free = []
    for line in subprocess.getoutput('btrfs fi show --raw %s | grep devid' % FILESYSTEM).split('\n'):
        x = line.split()
        devid = x[1]
        size = int(x[3])
        used = int(x[5])
        path = x[7]

        unallocated = size - used
        free.append(unallocated)

        if unallocated < least_empty_dev_unallocated:
            least_empty_dev_id = devid
            least_empty_dev_path = path
            least_empty_dev_unallocated = unallocated

    stdev = statistics.stdev(free)
    if stdev < STDEV_LIMIT:
        logging.info('Unallocated space stdev %s is below %s, exiting now.' % (sizeof_fmt(stdev), sizeof_fmt(STDEV_LIMIT)))
        sys.exit()
    else:
        logging.info('Unallocated space stdev %s is above %s, continuing...' % (sizeof_fmt(stdev), sizeof_fmt(STDEV_LIMIT)))

    logging.info('Balancing the least empty device: %s with %s unallocated' % (least_empty_dev_path, sizeof_fmt(least_empty_dev_unallocated)))

    cmd = 'btrfs balance start -ddevid=%s,limit=1 %s' % (least_empty_dev_id, FILESYSTEM)
    ret, out = subprocess.getstatusoutput(cmd)

    if ret != 0:
        logging.warning(out)
        time.sleep(30)
    else:
        logging.info(out)

def fib_sleep(index):
    seconds = fib(index)
    until = time.strftime("%H:%M:%S", time.localtime(time.time() + seconds))
    logging.info("Sleeping %ds until %s" % (seconds, until))
    time.sleep(seconds)

# ------------------

# Check that FILESYSTEM is btrfs
for part in psutil.disk_partitions():
    if part.mountpoint == FILESYSTEM:
        if part.fstype == 'btrfs':
            logging.info("Found btrfs filesystem mounted at %s" % FILESYSTEM)
            break
        else:
            logging.error("%s is not btrfs, exiting now." % FILESYSTEM)
            sys.exit(-1)
else:
    logging.error("Could not find btrfs at %s, exiting now." % FILESYSTEM)
    sys.exit(-1)

# All good, run the balancing loop
backoff = 0
while True:
    start = time.time()
    bal_chunk()
    duration = time.time() - start

    yeet = "Ouch!" if duration > CHUNK_TIMEOUT else "Nice!"
    logging.info("%s Last chunk took %ds" % (yeet, duration))

    # adjust a fibonacci backoff and sleep
    if (duration > CHUNK_TIMEOUT):
        if fib(backoff + 1) < MAX_SLEEP: backoff += 1
        fib_sleep(backoff)
    else:
        if backoff > 1: backoff -= 2 # that's optimism
        elif backoff > 0: backoff -= 1
        fib_sleep(backoff)
