#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)


# TODO
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


orphans = lines.flatMap(lambda line: links(line)) \
    .map(lambda a: (a.split("\t")[0], int(a.split("\t")[1]))) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda entry: (entry[1], entry[0])) \
    .sortByKey(False) \
    .map(lambda entry: (str(entry[1]), entry[0]))

results = orphans.take(10)
results.sort()
output = open(sys.argv[2], "w")
for orphan in results:
    output.write(str(orphan[0]) + "\t" + str(orphan[1]) + "\n")

# TODO
# write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()
