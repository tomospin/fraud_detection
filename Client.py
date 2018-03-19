#!/usr/bin/python
import socket             
import time
import datetime
import sys
import smtplib
from pushbullet import Pushbullet # Notifier
#Connect to Email server (GMAIL)
global g
g = smtplib.SMTP('smtp.gmail.com',587)
g.starttls()
g.login("EMAIL", "PASSWORD")
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("createFraudDetectionModel").setMaster("local[1]")

sc = SparkContext("local[2]", "FraudDetection")
ssc = StreamingContext(sc, 1)

# Read data and place in to dict (results from the model after training)
dataSet = sc.textFile("hdfs:/user/training/fraudDetectionModel4/part-00000")
dict_userid_meanPlus2sd = {}				# a dictionary keeps userid:mean+2sd
dict_userid_meanMinus2sd = {}				# another dictionary keeps userid:mean-2sd
for record in dataSet.toLocalIterator():			# each line from fraudDectectionModel4.csv in local
	field = record.split(",")						# each field of a line	
	userid = field[0]								# first field userid
	mean_plus_2sd = field[1]						# second field mean+2sd
	mean_minus_2sd = field[2]						# third field mean-2sd
	dict_userid_meanPlus2sd[userid] = mean_plus_2sd 	# add (userid,mean+2sd)to a dictionary
	dict_userid_meanMinus2sd[userid] = mean_minus_2sd	# add (userid,mean-2sd)to another dictionary

# Function for converting json file data and comparing it to the model to detect if a fraud occurs
def sendRecord(inputStreamLines):
	data = inputStreamLines.map(lambda x: json.loads(x.encode('utf-8'))).collect()
	for row in data:
		result = 0
		if int(row['amount']) > float(dict_userid_meanPlus2sd.get(row['userid'])) or int(row['amount']) < float(dict_userid_meanMinus2sd.get(row['userid'])):
			result = 1
		if result == 1:
			message = "FRAUD DETECTED!!!! Your account ID" + str(row['userid']) + " has " + str(row['action']) + " " + str(row['amount']) + " baht (It is above " + str(float(dict_userid_meanPlus2sd.get(row['userid']))) + " )"
			s.send(message)
			g.sendmail("EMAIL", "EMAIL", message)
			pb = Pushbullet("APIKEY")   # API ->> notifiy to chrome and mobile phone # old key= APIKEY
			push = pb.push_note("Fraud Detected", message)
			result = 0
# Recive Streaming for spark from the Stream and creating a new socket stream for showing the fraud detected
inputStreamLines = ssc.socketTextStream("localhost", 3222)
a = inputStreamLines.foreachRDD(sendRecord)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost',3223))
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate



