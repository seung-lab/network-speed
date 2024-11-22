from functools import partial
import multiprocessing as mp
import os
import time
import shutil
import sys
import time

import numpy as np

from cloudfiles import CloudFiles
from cloudfiles.lib import sip

from .encoding import transcode_image

SOURCES = [
    "mem://synthetic_images",
    "file://./nspeed_test_images",
    "file:///Volumes/scratch/ws9/nspeed_test_images",
]

DESTINATIONS = [
    "gs://seunglab/wms/speedtest/",
    "file://./nspeed_test_images_dest/",
]

NUM_PROCESSES = [ 1 ]

ENCODINGS = [ 'raw', 'jxl' ]

NFILES = 100

pid = os.getpid()

def print_headers():
    attrs = [
        "source",
        "destination",
        "encoding",
        "num_proc",
        "source_MB"
        "dest_MB",
        "seconds (wall clock)",
        "src MB/sec (wall clock)",
        "src Gbps (wall clock)",
    ]
    print("\t".join(attrs))

def setup_test_files():
    gppath = os.path.dirname(os.path.dirname(__file__))
    cfsrc = CloudFiles(f"file:///{gppath}/example_images")
    fnames = [ 
        fname for fname in list(cfsrc.list()) 
        if fname[0] != '.' 
    ]
    files = cfsrc.get(fnames)
    for file in files:
        file["path"], file["content"] = transcode_image(
            file["path"], file["content"], 
            encoding="bmp", level=5,
        )

    files *= ((NFILES + 3) // 4)

    for src in SOURCES:
        s = time.time()
        cfdest = CloudFiles(src)
        cfdest.puts([
            (f"{i:03}.bmp", f["content"])
            for i, f in enumerate(files) 
        ])
        elapsed = time.time() - s
        print(f"Initialized test images in {elapsed:.1f} seconds: {src}")

def worker(src, dest, encoding, paths):
    cfsrc = CloudFiles(src)
    cfdest = CloudFiles(dest)

    s = time.time()

    if encoding == "raw":
        cfsrc.transfer_to(dest, paths=paths)
    else:
        files = cfsrc.get(paths)
        x = [
            transcode_image(
                file["path"], file["content"], 
                encoding, level=100, effort=1
            )
            for file in files 
        ]
        cfdest.puts(x)
    elapsed = time.time() - s
    return elapsed

def _run_speed_test(src, dest, num_procs, encoding):

    fn = partial(worker, src, dest, encoding)

    filenames = [ 
        f"{i:03}.bmp" for i in range(NFILES) 
    ]

    ext = encoding.lower() if encoding != "raw" else 'bmp'
    filenames_dest = [ 
        f"{i:03}.{ext}" for i in range(NFILES) 
    ]

    if num_procs == 1:
        wall_clock_elapsed = fn(filenames)
    else:
        s = time.time()
        with mp.Pool(processes=num_procs) as pool:
            results = pool.map(fn, sip(filenames, 20))
        wall_clock_elapsed = time.time() - s

    source_bytes = CloudFiles(src).size(filenames, return_sum=True)
    dest_bytes = CloudFiles(dest).size(filenames_dest, return_sum=True)

    row = [
        src,
        dest,
        encoding,
        f"{num_procs}",
        f"{source_bytes / 1e6:.2f}",
        f"{dest_bytes / 1e6:.2f}",
        f"{wall_clock_elapsed:.2f}",
        f"{source_bytes / 1e6 / wall_clock_elapsed:.2f}",
        f"{source_bytes * 8 / 1e9 / wall_clock_elapsed:.2f}",
    ]
    print("\t".join(row))

def run_speed_tests():
    setup_test_files()
    print_headers()

    for num_procs in NUM_PROCESSES:
        for src in SOURCES:
            for dest in DESTINATIONS:
                for encoding in ENCODINGS:
                    _run_speed_test(src, dest, num_procs, encoding)
