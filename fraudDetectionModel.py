#!/usr/bin/python

import math

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("createFraudDetectionModel").setMaster("local[1]")

def prepareData(line):
    record = line.split(",")
    userid = record[3].encode("utf8")
    amount = int(record[5])
    return (userid, (amount, amount*amount ,1))

def createFraudDetectionModel(data):
    userid = data[0]
    value = data[1]
    n = value[2]
    mean = float(value[0]) / n
    sigmaxsquare = float(value[1])
    sd = math.sqrt((sigmaxsquare/n)-(mean*mean))
    return userid, mean + 2*sd, mean - 2*sd

sc = SparkContext(conf=conf)

dataSet = sc.textFile("hdfs:/user/training/ingested_csv/outputHugeData,hdfs:/user/training/ingested_csv/outputLogData,hdfs:/user/training/ingested_csv/sqlRecordsCsv")

results = dataSet.map(lambda line:prepareData(line)).reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1], a[2]+b[2])).map(lambda data:createFraudDetectionModel(data))


results.map(lambda (k, v1, v2): "{0},{1},{2}".format(k, v1, v2)).repartition(1).saveAsTextFile("hdfs:/user/training/fraudDetectionModel4")
