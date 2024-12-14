import os
import urllib.request
import ssl
from inspect import FullArgSpec
from itertools import count

from Tools.demo.sortvisu import distinct

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\Shuvait\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)
##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

#
# # 🔴  DATA PREPARATION
#
# data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]
#
# df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
# print(" === Task 2 RAW DATA === ")
# df.show()
#
# df1= df.select("col1")
# df1.show()
# df2= df.select("col2")
# df2.show()
# df3= df.select("col3")
# df3.show()
# df4= df.select("col4")
# df4.show()
#
# uniondf = df1.union(df2).union(df3).union(df4).withColumnRenamed("col1","col")
# print(" === Task 2 UNION === ")
# uniondf.show()

#
# #👉 Preparation Code
#
# data = [
#     (203040, "rajesh", 10, 20, 30, 40, 50)
# ]
#
# df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
# df.show()
#
#
# sumdf = df.withColumn("total",expr("telugu +english +maths +science +social"))
#
# sumdf.show()


#👉 Preparation Code
#
# data = [
#     (203040, "rajesh", 10, 20, 30, 40, 50)
# ]
#
# df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
# df.show()
#
#
# sumdf = df.withColumn("total",expr("telugu +english +maths +science +social"))
#
# sumdf.show()
#

### SCENARIO 1

# source_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (3, "C"),
#     (4, "D")
# ],1)
#
# target_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (4, "X"),
#     (5, "F")
# ],2)
#
# # Convert RDDs to DataFrames using toDF()
# df1 = source_rdd.toDF(["id", "name"])
# df2 = target_rdd.toDF(["id", "name1"])
#
# # Show the DataFrames
# df1.show()
# df2.show()
#
# print("===== FULL JOIN=====")
#
# fulljoin = df1.join( df2, ["id"], "full" )
# fulljoin.show()
#
# from pyspark.sql.functions import *
#
# print("=====NAME AND NAME 1 MATCH=====")
#
# fulljoin = df1.join( df2, ["id"], "full" )
# fulljoin.show()
#
# procdf = fulljoin.withColumn("status",expr("case when name=name1 then 'match' else 'mismatch' end"))
# procdf.show()
#
# print("=====FILTER MISMATCH=====")
#
# fildf = procdf.filter("status='mismatch'")
# fildf.show()
#
# print("=====NULL CHECKS=====")
#
# procdf1 = (
#
#     fildf.withColumn("status",expr("""
#                                             case
#                                             when name1 is null then 'New In Source'
#                                             when name is null  then 'New In target'
#                                             else
#                                             status
#                                             end
#
#
#                                             """))
#
# )
#
# procdf1.show()
#
#
# print("=====FINAL PROC=====")
#
#
# finaldf = procdf1.drop("name","name1").withColumnRenamed("status","comment")
#
# finaldf.show()


##SCENARIO
# data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]
#
# df1 = spark.createDataFrame(data,["food_id","food_item"])
# df1.show()
#
# ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]
#
# df2 = spark.createDataFrame(ratings,["food_id","rating"])
# df2.show()
#
#
# from pyspark.sql.functions import *
#
# leftjoin = df1.join(df2,["food_id"], "left").orderBy("food_id").withColumn("stats(out of 5)",expr("repeat('*',rating)"))
#
# leftjoin.show()


# CHILD,GRAND PARENTS,PARENTS SCENARIO

# data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]
#
# df = spark.createDataFrame(data, ["child", "parent"])
# df.show()
#
#
# df1  = df
#
# df2  = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
#
# df1.show()
# df2.show()
#
#
# joindf = df1.join(df2, df1["child"]==df2["parent1"] ,"inner")
# joindf.show()
#
# finaldf = (joindf.drop("parent1"))
# print("finaldf")
# finaldf.show()
#
# dffinal = (
#                 finaldf.withColumnRenamed("parent","parent1")
#                        .withColumnRenamed("child","parent")
#                        .withColumnRenamed("child1","child")
#                        .withColumnRenamed("parent1","grandparent")
#                        .select("child","parent","grandparent")
# )
#
# dffinal.show()


## SCENARIO
# data1 = [
#     (1, "A", "A", 1000000),
#     (2, "B", "A", 2500000),
#     (3, "C", "G", 500000),
#     (4, "D", "G", 800000),
#     (5, "E", "W", 9000000),
#     (6, "F", "W", 2000000),
# ]
# df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
# df1.show()
#
# data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
# df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
# df2.show()
#
#
#
# joindf = df1.join(df2,df1["dept_id"]==df2["dept_id1"] , "left")
#
# joindf.show()
#
#
# seldf = joindf.select("emp_id","name","dept_name","salary").orderBy("dept_name","salary")
# seldf.show()
#
#
# from pyspark.sql.functions import *
#
# exprdf =(
#     seldf.groupby("dept_name")
#     .agg(min("salary").alias("salary"))
#
# )
#
# exprdf.show()
#
#
#
# finaldf = exprdf.join(df1,["salary"],"inner").drop("dept_id")
#
# finaldf.select("emp_id","name","dept_name","salary").show()
#





## 🔴 Data Preparation code, 09-12-24 Scenario

# data = [
#     ('A', 'D', 'D'),
#     ('B', 'A', 'A'),
#     ('A', 'D', 'A')
# ]
#
# df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
#
# df.show()
#
# from pyspark.sql.functions import *
#
# ##sort on the count basis
# filterdf = (
#              df.groupBy("Won")
#               .count()
#                .withColumnRenamed("count","Count")
#            )
# filterdf.show()
#
# ##union of teamnames
# df1 = df.select("TeamA")
# df2=  df.select("TeamB")
# uniondf = df1.union(df2)
# uniondf.distinct().orderBy("TeamA").show()
#
# # ## final
# procdf1 = filterdf.join(uniondf,filterdf["Won"] == uniondf["TeamA"],"full").drop("Won").distinct().orderBy("TeamA")
# procdf1.show()
#
# finaldf = (
#
#     procdf1.withColumn("Count",expr("""
#                                             case
#                                             when Count is null  then 0
#                                             else
#                                             Count
#                                             end
#                                             """))
#
# )
# finaldf.show()
#
#
# dffinal = (
#                 finaldf.withColumnRenamed("TeamA","TeamName")
#                        .withColumnRenamed("Count","Won")
#                        .select("TeamName","Won")
# )
#
# dffinal.show()




##🔴 Today's Scenario

##🔴 *Data Preparation Code

# data1 = [
#     (1, "Henry"),
#     (2, "Smith"),
#     (3, "Hall")
# ]
# columns1 = ["id", "name"]
# rdd1 = sc.parallelize(data1,1)
# df1 = rdd1.toDF(columns1)
# df1.show()
# data2 = [
#     (1, 100),
#     (2, 500),
#     (4, 1000)
# ]
# columns2 = ["id", "salary"]
# rdd2 = sc.parallelize(data2,1)
# df2 = rdd2.toDF(columns2)
# df2.show()
# from pyspark.sql.functions import *
#
# joindf = df1.join(df2,["id"],"left").orderBy("id")
# joindf.show()
#
# finaldf = joindf.withColumn("salary",expr("case when salary is NULL then 0 else salary end"))
# finaldf.show()




##PIVOT SCENARIO

#colum values becomes column names
# from pyspark.sql.functions import *
# data = [
#     (101, "Eng", 90),
#     (101, "Sci", 80),
#     (101, "Mat", 95),
#     (102, "Eng", 75),
#     (102, "Sci", 85),
#     (102, "Mat", 90)
# ]
# columns = ["Id", "Subject", "Marks"]
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
# df.show()
#
#
# pivotdf = df.groupBy("Id").pivot("Subject").agg(first("Marks"))
# print("pivot table")
# pivotdf.show()




#🔴 Today's scenario


#👉 Data preparation code

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]
df = spark.createDataFrame(data, ["from", "to", "dist"])

df.show()

df1= df
from pyspark.sql.functions import *
df1.show()
df2= df1.withColumnRenamed("from","from1").withColumnRenamed("to","to1").withColumnRenamed("dist","dist1")
df2.show()

joindf = df1.join(df2,(df1["from"] == df2["to1"]) & (df1["to"] == df2["from1"]),"inner")
joindf.show()


groupdf = joindf.groupBy("from","to").agg(sum(col("dist") + col("dist1")).alias("roundtrip_dist"))
groupdf.show()


filterdf = groupdf.filter("from<to").orderBy("roundtrip_dist")
filterdf.show()







