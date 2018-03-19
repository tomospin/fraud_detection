CREATE EXTERNAL TABLE default.historicalTransactions (
	  id INT,
	  transaction_date STRING,
	  channel STRING,
	  userid STRING,
	  action STRING,
	  amount INT,
	  state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
LOCATION '/user/training/ingested_csv/outputHugeData';

 
CREATE EXTERNAL TABLE default.logRecords (
	  id INT,
	  transaction_date STRING,
	  channel STRING,
	  userid STRING,
	  action STRING,
	  amount INT,
	  state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
LOCATION '/user/training/ingested_csv/outputLogData';

CREATE EXTERNAL TABLE default.sqlRecords (
	  id INT,
	  transaction_date STRING,
	  channel STRING,
	  userid STRING,
	  action STRING,
	  amount INT,
	  state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
LOCATION '/user/training/ingested_csv/sqlRecordsCsv';


sudo -u hdfs hadoop fs -chown -R impala:supergroup /user/training/ingested_csv/outputHugeData
INSERT INTO historicaltransactions SELECT* FROM sqlRecords;
INSERT INTO default.historicalTransactions SELECT * FROM default.logRecords;