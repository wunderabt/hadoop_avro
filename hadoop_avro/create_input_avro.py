#!/usr/bin/env python3

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import glob
import os.path

schema = avro.schema.Parse(open("mp3s.avsc").read())

for raw_file in glob.glob("./raw_data/*.mp3"):
    avro_name = os.path.splitext(os.path.basename(raw_file))[0]
    avro_file = os.path.join("input", avro_name + ".avro")
    print("Processing {} --> {}".format(raw_file, avro_file))
    with open(raw_file, "rb") as raw_fh, \
         open(avro_file, "wb") as avro_fh, \
         DataFileWriter(avro_fh, DatumWriter(), schema, codec="deflate") as writer:
        writer.append({"name": avro_name, "file": raw_fh.read()})
