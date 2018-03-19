#!/usr/bin/python
import json

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("json_to_csv").setMaster("local[1]")

def jsonToCsv(line):
    data = json.loads(line)
    return data["id"] + "," + data["timestamp"] + "," + data["channel"] + "," + data["userid"] + "," + data["action"] + "," + str(data["amount"]) + "," + data["location"]

sc = SparkContext(conf=conf)

dataSet = sc.textFile("hdfs:/user/training/json_log_data")

csv = dataSet.map(lambda line:jsonToCsv(line))

#results = csv.collect()
#for data in results:
#    print data
csv.saveAsTextFile("hdfs:/user/training/outputLogData")
