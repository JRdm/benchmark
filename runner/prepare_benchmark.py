# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prepare the big data benchmark on one or more EC2 or Redshift clusters.

   This script will copy the appropriately sized input data set from s3
   into the provided cluster or clusters.
"""

import subprocess
import sys
from sys import stderr
from optparse import OptionParser
import os
import time
from pg8000 import DBAPI
import pg8000.errors
import engine, benchmark
import inspect

# A scratch directory on your filesystem
LOCAL_TMP_DIR = "/tmp"

#Detect all engine packages available
ENGINES = [ c 
              for name, cls in inspect.getmembers(engine)
                for n,c in inspect.getmembers(cls)
                  if inspect.isclass(c) and issubclass(c, engine.Engine) and c is not engine.Engine
          ]

#Detect all benchmark packages available
BENCHMARKS = [ c 
              for name, cls in inspect.getmembers(benchmark)
                for n,c in inspect.getmembers(cls)
                  if inspect.isclass(c) and issubclass(c, benchmark.Benchmark) and c is not benchmark.Benchmark
             ]

### Runner ###
def parse_args():
  parser = OptionParser(usage="prepare_benchmark.py [options]")

  for e in ENGINES:
    e.parser_options(parser)

  for b in BENCHMARKS:
    b.parser_options(parser)

  parser.add_option("-d", "--aws-key-id",
      help="Access key ID for AWS")
  parser.add_option("-k", "--aws-key",
      help="access key for aws")

  parser.add_option("--setup-engine", action="store_true", default=False,
      help="Perform setup for the engine")
  parser.add_option("--load-data", action="store_true", default=False,
      help="Copy data from S3")
  parser.add_option("--create-schema", action="store_true", default=False,
      help="Create tables, including converting formats")
  parser.add_option("--query", action="store_true", default=False,
      help="Run the benchmarking queries")

  parser.add_option("--orc-file", action="store_true", default=False,
      help="Produce ORCFile tables")
  parser.add_option("--rc-file", action="store_true", default=False,
      help="Produce RCFile tables")
  parser.add_option("--parquet", action="store_true", default=False,
      help="Produce Parquet tables")

  parser.add_option("--benchmark-text", action="store_true", default=False,
      help="Benchmark with text files")

  (opts, args) = parser.parse_args()

  under_test = [ e 
                   for e in ENGINES
                     if e.is_enabled(opts)
               ]
 
  tests = [
            b
              for b in BENCHMARKS
                if b.is_enabled(opts)  
          ]  
  
  if (len(under_test) == 0 or len(tests) == 0):
    parser.print_help()
    sys.exit(1)

  engines = []
  benchmarks = []

  for engine in under_test:
    engines += [engine(opts)]

  for benchmark in tests:
    benchmarks += [benchmark(opts)]
    
  return engines, benchmarks, opts

def print_percentiles(in_list):
  print "Got list %s" % in_list
  def get_pctl(lst, pctl):
    return lst[int(len(lst) * pctl)]
  in_list = sorted(in_list)
  print "%s\t%s\t%s" % (
    get_pctl(in_list, 0.05),
    get_pctl(in_list, .5),
    get_pctl(in_list, .95)
  )

def main():
  engines, benchmarks, opts = parse_args()
  
  if opts.setup_engine:
    for engine in engines:
      print "Preparing for %s" % engine.name
      engine.setup_env()

  if opts.load_data:
    for benchmark in benchmarks:
      for engine in engines:
        print "Loading data for %s into %s" % (benchmark.name, engine.name) 
        benchmark.load_data(engine)

  if opts.create_schema:
    for benchmark in benchmarks:
      for engine in engines:
        print "Creating tables for %s on %s" % (benchmark.name, engine.name)
        benchmark.create_tables(engine)
        if opts.orc_file:
          print "Creating ORCFiles for %s on %s" % (benchmark.name, engine.name)
          benchmark.create_orcfiles(engine)
        if opts.rc_file:
          print "Creating RCFiles for %s on %s" % (benchmark.name, engine.name)
          benchmark.create_rcfiles(engine)
        if opts.parquet:
          print "Creating Parquet for %s on %s" % (benchmark.name, engine.name)
          benchmark.create_parquet(engine)

  if opts.query:
    for benchmark in benchmarks:
      for engine in engines:
        if opts.benchmark_text:
          print ("Running %s on %s (plain-text)" % (benchmark.name, engine.name))
          benchmark.run_test(engine, "")
        if opts.parquet:
          if engine.is_format_supported(engine, "parquet"):
            benchmark.run_test(engine, "parquet")          
          else:
            print ("Skipping %s on %s (parquet is not supported)" % (benchmark.name, engine.name))
        if opts.orc_file:
          if engine.is_format_supported(engine, "orc"):
            benchmark.run_test(engine, "orc")          
          else:
            print ("Skipping %s on %s (ORCFile is not supported)" % (benchmark.name, engine.name))
        if opts.rc_file:
          if engine.is_format_supported(engine, "rc"):
            benchmark.run_test(engine, "rc")          
          else:
            print ("Skipping %s on %s (RCFile is not supported)" % (benchmark.name, engine.name))

if __name__ == "__main__":
  main()
