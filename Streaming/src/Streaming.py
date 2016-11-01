'''
Created on Oct 31, 2016

@author: lokananda
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType 

spark = SparkSession\
    .builder\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()
    
    
# Create DataFrame representing the stream of input lines from connection to localhost:9999
# lines = spark\
#    .readStream\
#    .format('socket')\
#    .option('host', 'localhost')\
#    .option('port', 9999)\
#    .load()


userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark\
    .readStream\
    .option("sep", ";")\
    .schema(userSchema)\
    .csv("/split_dataset_monitored") 
    
    
# Split the lines into words
words = csvDF.select(
   explode(
       split(csvDF.value, ' ')
   ).alias('word')
)
 
# Generate running word count
wordCounts = words.groupBy('word').count()
 
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()
# 
# query.awaitTermination()