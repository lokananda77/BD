'''
Created on Oct 31, 2016

@author: lokananda
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, IntegerType 
from pyspark.sql import SQLContext
#from pyspark import SparkContext

spark = SparkSession\
    .builder\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()
    
#ipUserIds = raw_input("Enter the userIds: ")
ipUserIds = "36371 14454 1298"


userIdSchema = StructType().add("userA", "integer").add("dummy", "integer")

sqlCtx = SQLContext(spark)
print list(map((lambda x:(int(x),)), ipUserIds.split(" ")))
#ipStaticDF = sqlCtx.createDataFrame(list(map((lambda x:((x))), ipUserIds.split(" "))), userIdSchema)

ipStaticDF = spark.createDataFrame([('a', 36371), ('a',14454), ('a', 1298)], ['d', 'userA'])
ipStaticDF.printSchema()
userSchema = StructType().add("userA", "integer").add("userB", "integer").add("timestamp", "timestamp").add("interaction", "string")

csvDF = spark\
    .readStream\
    .option("sep", ",")\
    .schema(userSchema)\
    .csv("/split_dataset_monitored") 

userTweetsOfSpecificUsers = csvDF.join(ipStaticDF, ["userA"]);
    
# Generate running word count
csvDF.printSchema()
#intCounts = csvDF.select("userB, interaction").where("interaction = 'MT'")
#intCounts = csvDF.groupBy(window(csvDF.timestamp, '10 minutes', '5 minutes'), csvDF.interaction).count()
#MTDataFrame = spark.sql("select interaction,count(*) as total from wordCounts group by interaction") 
query = userTweetsOfSpecificUsers\
    .writeStream\
    .format('console')\
    .start()
# 
query.awaitTermination()
