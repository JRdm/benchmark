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
from optparse import OptionParser, OptionGroup
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

  parser.add_option("-d", "--aws-key-id",
      help="Access key ID for AWS")
  parser.add_option("-k", "--aws-key",
      help="access key for aws")

  action_group = OptionGroup(parser, "Actions")
  action_group.add_option("--setup-engine", action="store_true", default=False,
      help="Perform setup for the engine")
  action_group.add_option("--load-data", action="store_true", default=False,
      help="Copy data from S3")
  action_group.add_option("--create-schema", action="store_true", default=False,
      help="Create tables, including converting formats")
  action_group.add_option("--query", action="store_true", default=False,
      help="Run the benchmarking queries")
  parser.add_option_group(action_group)

  for e in ENGINES:
    e.parser_options(parser)

  benchmark_group = OptionGroup(parser, "Benchmarks")
  for b in BENCHMARKS:
    b.parser_options(benchmark_group)

  parser.add_option_group(benchmark_group)
  format_group = OptionGroup(parser, "File Formats", "Create tables and test against supported file types")

  format_group.add_option("--orc-file", action="store_true", default=False,
      help="Produce and test against ORCFile tables")
  format_group.add_option("--rc-file", action="store_true", default=False,
      help="Produce and test against RCFile tables")
  format_group.add_option("--parquet", action="store_true", default=False,
      help="Produce and test agaisnt Parquet tables")

  format_group.add_option("--benchmark-text", action="store_true", default=False,
      help="Run benchmarks with the source text files")

  parser.add_option_group(format_group)

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
  
  if len(under_test) == 0:
    print 'No engines chosen to test!'

  if len(under_test) == 0:
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
          benchmark.run_test(engine, "text")
        if opts.parquet:
          if engine.is_format_supported(engine, "parquet"):
            benchmark.run_test(engine, "parquet")          
          else:
            print ("Skipping %s on %s (parquet is not supported)" % (benchmark.name, engine.name))
        if opts.orc_file:
          if engine.is_format_supported(engine, "orcfile"):
            benchmark.run_test(engine, "orcfile")          
          else:
            print ("Skipping %s on %s (ORCFile is not supported)" % (benchmark.name, engine.name))
        if opts.rc_file:
          if engine.is_format_supported(engine, "rcfile"):
            benchmark.run_test(engine, "rcfile")          
          else:
            print ("Skipping %s on %s (RCFile is not supported)" % (benchmark.name, engine.name))

if __name__ == "__main__":
  main()
