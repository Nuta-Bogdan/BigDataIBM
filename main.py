import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("Erasmus.csv", header=True)

# tema 1

# frecventa valori
d3 = df.groupBy(["Receiving Country Code", "Sending Country Code"]).count()
# sortare
d4 = d3.orderBy(["Receiving Country Code", "Sending Country Code"])
d4.filter((d4["Receiving Country Code"] == "LV") | (d4["Receiving Country Code"] == "MK") |(d4["Receiving country code"] == "MT")).show()