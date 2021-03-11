#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO

leagueIds = sc.textFile(sys.argv[2], 1)

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

leagueIds = leagueIds.collect()

popular = lines.flatMap(lambda line: links(line)) \
    .map(lambda a: (a.split("\t")[0], int(a.split("\t")[1]))) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda entry: entry[0] in leagueIds)\
    .map(lambda entry: (entry[1], entry[0])) \
    .sortByKey() \
    .map(lambda entry: (str(entry[1]), entry[0]))\
    .collect()

count = 0
rank = 0
nextrank = 0
results = []
for entry in popular:
    if count != entry[1]:
        rank = nextrank
    count = entry[1]
    nextrank = nextrank + 1
    results.append((entry[0], rank))
print(popular)
print(results)
results.sort()
#TODO

output = open(sys.argv[3], "w")
for entry in results:
    output.write(str(entry[0]) + "\t" + str(entry[1]) + "\n")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

