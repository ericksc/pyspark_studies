# Import the SparkSession class from PySpark
from pyspark.sql import SparkSession

# Create a SparkSession
# SparkSession is the entry point to programming with Spark using the DataFrame API
spark = (
    SparkSession.builder
    .appName("ExampleJob")     # Set the name of the Spark application (shows in logs/UI)
    .getOrCreate()             # Gets an existing Spark session or creates a new one
    )            

# Sample data - a list of tuples, where each tuple is a row of data
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]

# Create a DataFrame from the sample data
# Each tuple becomes a row, and column names are provided as a list
df = spark.createDataFrame(data, ["name", "age"])

# Show the content of the DataFrame in a tabular format
df.show()

# Stop the SparkSession to release resources
spark.stop()
