#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext


def links(line):
    tokens = line.split(':')
    parent = tokens[0].strip()
    children = tokens[1].strip().split(' ')
    pages = []
    for child in children:
        child = child.strip()
        if parent != child:
            pages.append(child + "\t1")
    pages.append(parent + "\t0")
    return pages


conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

# TODO

orphans = lines.flatMap(lambda line: links(line)) \
    .map(lambda a: (a.split("\t")[0], int(a.split("\t")[1])))\
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda entry: entry[1] == 0)\
    .sortByKey()


print("==========================================")
print(orphans.take(10))
print("==========================================")
output = open(sys.argv[2], "w")
for orphan in orphans.collect():
    output.write(orphan[0] + "\n")
# TODO
# write results to output file. Foramt for each line: (line+"\n")

sc.stop()
