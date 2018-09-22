from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import sys

spark = SparkSession.builder.appName("Try1").config("spark.some.config.option", "some-value").getOrCreate()

