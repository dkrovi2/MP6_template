#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext


def splitline(line, delimiters):
    source = line
    for ch in delimiters:
        # print("ch="+ch+", line="+line)
        line = line.replace(ch, ' ')
    target = line.split(' ')
    # print("Source="+source+"\n"+"Target="+line)
    return target

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stopwords = []
delimiters = []

with open(stopWordsPath) as f:
    stopwords = [x.strip().lower() for x in f.readlines()]

with open(delimitersPath) as f:
    delimiters = [x for x in f.readlines()]


conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
words = lines.flatMap(lambda line: splitline(line.lower(), delimiters[0]))\
    .filter(lambda word: word not in stopwords)\
    .filter(lambda word: word != '')\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda x: (x[1], x[0]))\
    .sortByKey(False)


results = []

for entry in words.take(10):
    result = list(entry)
    results.append(result[1] + "\t" + str(result[0]))

results.sort()
outputFile = open(sys.argv[4], "w")

for entry in results:
    outputFile.write(entry + "\n")
# TODO
# write results to output file. Format for each line: (line +"\n")

sc.stop()
