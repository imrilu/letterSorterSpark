from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, StringType

schema = StructType([
    StructField("letter", StringType()),
    StructField("value", IntegerType())
])

spark = SparkSession.builder.getOrCreate()
spark.read.csv("homeAssignment.csv", schema=schema)\
    .repartition("letter")\
    .sortWithinPartitions("value")\
    .select("value")\
    .write.mode("overwrite").csv("output/")
