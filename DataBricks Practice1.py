# Databricks notebook source
rdd=sc.range(1,200)
rdd.collect()

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

sc.getConf().getAll()

# COMMAND ----------

sc._conf.get('spark.driver.memory')

# COMMAND ----------

sc._conf.get('spark.master')

# COMMAND ----------

sc.applicationId

# COMMAND ----------

sc.defaultMinPartitions

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

sc.serializer

# COMMAND ----------

sc._batchSize

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8])

# COMMAND ----------

rdd.take(1)

# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/first_text.txt")

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd1=rdd.flatMap(lambda x: x.split(" "))
rdd2=rdd1.map(lambda x: (x,1))

# COMMAND ----------

rdd3=rdd2.reduceByKey(lambda x,y:x+y)

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

sc._conf.set("spark.sql.shuffle.partitions","300")

# COMMAND ----------

rdd = sc.textFile("dbfs:/FileStore/tables/first_text-1.txt")
rdd1=rdd.flatMap(lambda x: x.split(" "))
rdd2=rdd1.map(lambda x: (x,1))
rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
rdd3.collect()

# COMMAND ----------

rdd = sc.textFile('dbfs:/FileStore/tables/data.txt')

# COMMAND ----------

col =["Name","Domain","Year"]

# COMMAND ----------

def func(line):
    line=line.strip("\n\r")
    line_fields=line.split(",")
    data = dict(zip(col,line_fields))
    return data

# COMMAND ----------

func("Naman,Engineer,2020")

# COMMAND ----------

rdd2 = rdd.map(func)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3=rdd2.toDF()

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

rdd3.show()

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

def func_1(line):
    line["Name"]=line["Name"].upper()
    return line

# COMMAND ----------

rdd4=rdd2.map(func_1)

# COMMAND ----------

rdd4.collect()

# COMMAND ----------


