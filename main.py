import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("Erasmus.csv", header=True)

# tema 1
# selectare coloane
d2 = df.select(["Receiving Country Code", "Sending Country Code"])
# frecventa valori
d3 = d2.groupBy(["Receiving Country Code", "Sending Country Code"]).count()
# sortare
d4 = d3.orderBy(["Receiving Country Code", "Sending Country Code"])
d4.show(n=df.count(), truncate=False)
