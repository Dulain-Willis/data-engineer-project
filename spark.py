from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("sandbox").getOrCreate()

# Read the raw text from the part files (each file is one big JSON object)
rdd = spark.sparkContext.textFile(
    "/data/steamspy-dev-raw/steamspy_json/b215c88d-5e00-45b8-9c89-4182c0964fef/part.*"
)

# These files are each a single long line of JSON, so just take the first element
text = rdd.collect()[0]

# Parse JSON into a Python dict: { "appid_str": {record...}, ... }
parsed = json.loads(text)

# We only care about the values (the game records), not the dict keys
records = list(parsed.values())

# Create a DataFrame from the list of dicts
df = spark.createDataFrame(records)

df.show(20, truncate=False)
df.printSchema()
