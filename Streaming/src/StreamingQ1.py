'''
Created on Oct 31, 2016

@author: lokananda
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import split
from pyspark.sql.types import StructType 

spark = SparkSession\
    .builder\
    .appName("CS-838-Assignment2-PartB")\
    .getOrCreate()
    
userSchema = StructType().add("userA", "integer").add("userB", "integer").add("timestamp", "timestamp").add("interaction", "string")
csvDF = spark\
    .readStream\
    .option("sep", ",")\
    .schema(userSchema)\
    .csv("/split_dataset_monitored") 
    
    
#words = csvDF.select("userA")
 
# Generate running word count
#wordCounts = words.groupBy('name').count()
csvDF.printSchema()
intCounts = csvDF.groupBy(window(csvDF.timestamp, '10 minutes', '5 minutes'), csvDF.interaction).count()
#MTDataFrame = spark.sql("select interaction,count(*) as total from wordCounts group by interaction") 
query = intCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()
# 
query.awaitTermination()
