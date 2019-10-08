CREATE EXTERNAL TABLE IF NOT EXISTS test.purchase_history (
	UID string,
	DAY string,
	CATEGORY int,
	TOTAL float
)
COMMENT 'external purchase table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/hive/warehouse/test.db/CDNOW';

DROP TABLE IF EXISTS test.purchase_history;