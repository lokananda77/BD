from __future__ import print_function
'''
Created on Oct 29, 2016

@author: lokananda
'''
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
"""


import re
import sys
from operator import add
from pyspark import SparkConf, SparkContext

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: pagerank <file> <iterations>", file=sys.stderr)
#         exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    sparkConf = SparkConf().setAppName("CS-838-Assignment2-PartA-1")\
    .setMaster("spark://10.254.0.160:7077")\
    .set("spark.executor.memory", "1G")\
    .set("spark.eventLog.enabled", "true")\
    .set("spark.evenLog.dir", "file:///tmp/test")\
    .set("spark.driver.memory", "4G")\
    .set("spark.executor.cores", 4)\
    .set("spark.task.cpus", 1)
    
    sc = SparkContext(conf = sparkConf)
    
    # Initialize the spark context.
    #spark = SparkSession\
    #    .builder\
    #    .appName("CS-838-Assignment2-PartA-1")\
    #    .config("spark.executor.memory", "1G")\
    #    .getOrCreate()

    
    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #
    lines = sc.textFile("hdfs:///web-BerkStan1.txt");
    #lines = sc.textFile("web-BerkStan.txt")
    #lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().partitionBy(16).groupByKey()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))
    sc.stop()
