from pyspark.sql import SparkSession

# the spark session
spark = SparkSession.builder.master("spark://spark:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()


# load the data
path_to_data = "/app/stack/"

answers = spark.read.csv(f"{path_to_data}Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
badges = spark.read.csv(f"{path_to_data}Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
comments = spark.read.csv(f"{path_to_data}Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questions = spark.read.csv(f"{path_to_data}Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questionsLinks = spark.read.csv(f"{path_to_data}QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questionsTags = spark.read.csv(f"{path_to_data}QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
tags = spark.read.csv(f"{path_to_data}Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
users = spark.read.csv(f"{path_to_data}Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votes = spark.read.csv(f"{path_to_data}Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votesTypes = spark.read.csv(f"{path_to_data}VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')


# tables = ['Answers', 'Badges', 'Comments', 'Questions', 'QuestionsLinks', 'QuestionsTags', 'Tags', 'Users', 'Votes', 'VotesTypes']
# for table in tables:
#     print(f"Loading {table}...")
#     df = spark.read.csv(f"../../stackBenck/db/data/{table}.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # df.createOrReplaceTempView(table.lower())


# write the data to parquet
answers.write.parquet(f'{path_to_data}answers_parquet')
badges.write.parquet(f'{path_to_data}badges_parquet')
comments.write.parquet(f'{path_to_data}comments_parquet')
questions.write.parquet(f'{path_to_data}questions_parquet')
questionsLinks.write.parquet(f'{path_to_data}questionsLinks_parquet')
questionsTags.write.parquet(f'{path_to_data}questionsTags_parquet')
tags.write.parquet(f'{path_to_data}tags_parquet')
users.write.parquet(f'{path_to_data}users_parquet')
votes.write.parquet(f'{path_to_data}votes_parquet')
votesTypes.write.parquet(f'{path_to_data}votesTypes_parquet')

# Partitioning parquet files
# answers.write.parquet(f'{path_to_data}answers_parquet_partitioned_creationdate', partitionBy='creationdate')
# questions.write.parquet(f'{path_to_data}questions_parquet_partitioned_creationdate', partitionBy='creationdate')
# comments.write.parquet(f'{path_to_data}comments_parquet_partitioned_creationdate', partitionBy='creationdate')    


"""
# Using the DataFrame function write.parquet(out folder), export the titles DataFrame to Parquet
titles.write.parquet('/app/titles_parquet') # Dá erro se tentar escrever isto e já existir a pasta com o mesmo nome

# Export the DataFrame again, using gzip compression, by providing the compression=’gzip’ parameter to write.parquet.
titles.write.parquet('/app/titles_parquet_gzip', compression='gzip')

# Export the DataFrame again, partitioned by the startYear column (partitionBy=’startYear’)
titles.write.parquet('/app/titles_parquet_partitioned', partitionBy='startYear')
"""
