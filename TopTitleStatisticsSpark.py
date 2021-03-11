#!/usr/bin/env python
import sys
import math
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1],1)

#TODO
entries = lines.map(lambda line: line.strip().split("\t")[1]).collect()

count = 0
mean = 0
sum = 0
min = sys.maxsize
max = 0
for e in entries:
    entry = int(e)
    count = count + 1
    sum = sum + entry
    if max < entry:
        max = entry
    if min > entry:
        min = entry

mean = int(sum / count)
sumOfSquares = 0
for entry in entries:
    sumOfSquares = sumOfSquares + math.pow(int(entry) - mean, 2)


variance = int(sumOfSquares / count)


outputFile = open(sys.argv[2],"w")
'''
TODO write your output here
write results to output file. Format
'''
outputFile.write('Mean\t%s\n' % mean)
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % variance)

sc.stop()

