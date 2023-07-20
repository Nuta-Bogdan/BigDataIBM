import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.18.jar").getOrCreate()

df = spark.read.csv("Erasmus.csv", header=True)

# tema 1

# frecventa valori
d3 = df.groupBy(["Receiving Country Code", "Sending Country Code"]).count()
# sortare
d4 = d3.orderBy(["Receiving Country Code", "Sending Country Code"])
d4.filter((d4["Receiving Country Code"] == "LV") | (d4["Receiving Country Code"] == "MK") | (
        d4["Receiving country code"] == "MT")).show()

# tema 2

# conexiune

jdbc_url = "jdbc:mysql://localhost:3306/IBM"
connection_properties = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# scriere in baza de date

nationCodes = ["LV", "RO", "UK"]
for code in nationCodes:
    d2 = df.filter(df["Receiving Country Code"] == code).drop("Receiving Country Code")
    d2.write.jdbc(url=jdbc_url, table=code, mode="overwrite", properties=connection_properties)
