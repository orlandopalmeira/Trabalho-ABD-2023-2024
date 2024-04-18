from pyspark.sql import SparkSession 

# Create a SparkSession 
spark = SparkSession.builder.appName("range_partitioning").getOrCreate() 

# Create a sample DataFrame 
df = spark.createDataFrame([ 
	(1, "Alice", 25), 
	(2, "Bob", 30), 
	(3, "Charlie", 35), 
	(4, "Dave", 40), 
	(5, "Eve", 45), 
	(6, "Frank", 50) 
], ["id", "name", "age"]) 

# Perform range partitioning on the 
# DataFrame based on the "age" column 
df = df.repartitionByRange(3, "age") 

# Print the elements in each partition 
print(df.rdd.glom().collect()) 
