from pyspark.sql import SparkSession

# the spark session
spark = SparkSession.builder.master("spark://spark:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

# test that spark is working
df = spark.range(5).toDF("number")
df.show()

# load the title.basics dataset
titles = spark.read.csv("/app/imdb/title.basics.tsv", header=True, inferSchema=True, sep="\t")

# get a random sample and print the first 5
titles.sample(0.01).show(5)

# get the number of entries in the dataframe
print(f'Number of titles = {titles.count()}')

# create a view to use with SQL
titles.createOrReplaceTempView("titles")

# query: get the release year of all titles named Oppenheimer
spark.sql("select startYear from titles where primaryTitle = 'Oppenheimer'").show()

# 2. query: get the number of titles per titleTypes
# spark.sql("select titleType, count(*) as primaryTitle from titles group by titleType").show()
titles.groupBy("titleType").count().show() # untested

# 3. Using the DataFrame function write.parquet(out folder), export the titles DataFrame to Parquet
# titles.write.parquet('/app/titles_parquet') # Dá erro se tentar escrever isto e já existir a pasta com o mesmo nome

# 4. Export the DataFrame again, using gzip compression, by providing the compression=’gzip’ parameter to write.parquet.
# titles.write.parquet('/app/titles_parquet_gzip', compression='gzip')

# 5. Export the DataFrame again, partitioned by the startYear column (partitionBy=’startYear’)
# titles.write.parquet('/app/titles_parquet_partitioned', partitionBy='startYear')

# 6. Using the previous export and the function spark.read.parquet(in folder), compute the number of titles released in 2023.
titles_df = spark.read.parquet('/app/titles_parquet_partitioned')
titles_2023 = titles_df.filter(titles_df.startYear == 2023)
print("Titles from 2023:")
titles_2023.show()
print("Number of titles released in 2023: " + str(titles_2023.count()))