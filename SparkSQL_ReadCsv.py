# load csv as DataFrame
df = spark.read.format("csv").option("header", "false").load("hdfs:///tmp/CDNOW_clean.csv")
df.show()