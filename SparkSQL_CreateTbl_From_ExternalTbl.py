######################
#  CREATE ORC TABLE  #
######################
# methode 1 (OK)
# CREATE TABLE STORED AS ORC & INSERT INTO
CREATE TABLE city_orc(cityid int, cityname string) STORED AS ORC;
INSERT INTO TABLE city_orc SELECT * FROM city;

# methode 2 (OK)
# CREATE TABLE AS SELECT STORED AS ORC
CREATE TABLE imsi_orc STORED AS ORC AS select * from imsi ;

# methode 1 (marche pas)
# create table & LOAD DATA INPATH
# 1) create table 
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_history_internal (UID string, DAY string, CATEGORY int, TOTAL float) USING hive")

# 2) load data
# les suivants ne marchent pas !!!
spark.sql("LOAD DATA LOCAL INPATH '/apps/hive/warehouse/test.db/CDNOW/CDNOW_clean.csv' INTO TABLE test.purchase_history_internal")

spark.sql("LOAD DATA LOCAL INPATH 'hdfs://{hdpcldbkcluster-m-3-20180611010402.c.corporate-hdp-sandbox.internal}:8020/apps/hive/warehouse/test.db/CDNOW/CDNOW_clean.csv' INTO TABLE test.purchase_history_internal")

spark.sql("LOAD DATA LOCAL INPATH 'hdfs:///tmp/CDNOW_clean.csv' INTO TABLE test.purchase_history_internal") 
# --------------------------------------

# methode 2 (OK)
# CREATE TABLE & INSERT SELECT
# create table
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_history_internal (UID string, DAY string, CATEGORY int, TOTAL float) USING hive")
# insert data from external table
spark.sql("INSERT OVERWRITE TABLE test.purchase_history_internal SELECT * FROM test.purchase_history")


# methode 3 (OK)
# CREATE TABLE AS SELECT
# create hive internal table as select from a hive external table
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_history_internal2 as select * from test.purchase_history")

spark.sql("SELECT * FROM test.purchase_history_internal2").show()

# aggregations
spark.sql("SELECT category,COUNT(*) FROM test.purchase_history_internal2 GROUP BY category ORDER BY category").show()

spark.sql("SELECT DISTINCT category FROM test.purchase_history_internal2").show()
# show() show 20 results by default, if we want to see more, use the following commande
spark.sql("SELECT DISTINCT category FROM test.purchase_history_internal2 ORDER BY category").show(50, truncate = False)

# create a datamart(calculé) from hive table
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_cat_amont as SELECT category,COUNT(*) AS quantity FROM test.purchase_history GROUP BY category ORDER BY category")


# methode 4 (OK) ------------------------------------
# from one source table insert multi dest tables
# create user table with their total depense
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_user (UID string, TOTAL float) USING hive")
# create category with its total turnover
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_category (CATEGORY int, TOTAL float) USING hive")

# insert data from the same source table
spark.sql("FROM test.purchase_history INSERT OVERWRITE TABLE test.purchase_user SELECT UID, sum(TOTAL) GROUP BY UID INSERT OVERWRITE TABLE test.purchase_category SELECT CATEGORY, sum(TOTAL) GROUP BY CATEGORY")


# methode 5 create partition and bucket table
CREATE TABLE IF NOT EXISTS test.purchase_history_partitioned_bucket (UID string, DAY string, CATEGORY int, TOTAL float)partitioned by(YEAR smallint) Clustered by(UID) sorted by(DAY) into 4 buckets;

INSERT INTO test.purchase_history_partitioned_bucket PARTITION(YEAR) SELECT * FROM test.purchase_history_internal3; 



# ??????????????????????????????????????????????????????????
# this same code not work in HIVE but work well in SPARK ???
# ??????????????????????????????????????????????????????????
# create table from external table
# the following 3 exemples
# 1)
# ----------------------------------------------------------
# (KO) works only when FIELDS TERMINATED BY ','
CREATE EXTERNAL TABLE IF NOT EXISTS test.purchase_history_spark (
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
LOCATION '/user/xiaoxiao/CDNOW-test';
# (OK)
spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS test.purchase_history_spark (UID string,DAY string,CATEGORY int,TOTAL float) COMMENT 'external purchase table' ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/user/xiaoxiao/CDNOW-test' ")
# ----------------------------------------------------------

# 2)
# ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS test.purchase_history_internal3 like test.purchase_history;
# (KO)
INSERT OVERWRITE TABLE test.purchase_history_internal3 SELECT uid, day, category, total, substr(day,0,4) as year FROM test.purchase_history ; 
# (OK)
spark.sql("INSERT OVERWRITE TABLE test.purchase_history_internal3 SELECT uid, day, category, total, substr(day,0,4) as year FROM test.purchase_history")
# -----------------------------------------------------------

# 3)
# ----------------------------------------------------------
# (KO)
CREATE TABLE IF NOT EXISTS test.purchase_history_internal4 as SELECT uid, day, category, total, substr(day,0,4) as year FROM test.purchase_history
# (OK)
spark.sql("CREATE TABLE IF NOT EXISTS test.purchase_history_internal4 as SELECT uid, day, category, total, substr(day,0,4) as year FROM test.purchase_history")
# ----------------------------------------------------------


ALTER TABLE test.purchase_history add columns(year string);

ALTER TABLE test.purchase_history replace columns(UID string, DAY string, CATEGORY int, TOTAL float);

ALTER TABLE test.purchase_history SET SERDEPROPERTIES('field.delim'=',');

ALTER TABLE test.purchase_history SET LOCATION 'hdfs:///user/xiaoxiao/CDNOW';






