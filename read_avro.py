#!/usr/bin/env python3

import json
import sys

import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

with open(sys.argv[1], "rb") as avro_fh, DataFileReader(avro_fh, DatumReader()) as reader:
    for mp3 in reader:
        print(json.dumps(mp3))