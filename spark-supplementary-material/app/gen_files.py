from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, year

def count_rows(iterator):
    yield len(list(iterator))

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')

# the spark session
spark = SparkSession.builder.master("spark://spark:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()


# load the data
path_to_data = "/app/stack/"

answers = spark.read.csv(f"{path_to_data}Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questions = spark.read.csv(f"{path_to_data}Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
comments = spark.read.csv(f"{path_to_data}Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
badges = spark.read.csv(f"{path_to_data}Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questionsLinks = spark.read.csv(f"{path_to_data}QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questionsTags = spark.read.csv(f"{path_to_data}QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
tags = spark.read.csv(f"{path_to_data}Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
users = spark.read.csv(f"{path_to_data}Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votes = spark.read.csv(f"{path_to_data}Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votesTypes = spark.read.csv(f"{path_to_data}VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')


# write the data to basic parquet 
# answers.write.parquet(f'{path_to_data}answers_parquet')
# badges.write.parquet(f'{path_to_data}badges_parquet')
# comments.write.parquet(f'{path_to_data}comments_parquet')
# questions.write.parquet(f'{path_to_data}questions_parquet')
# questionsLinks.write.parquet(f'{path_to_data}questionsLinks_parquet')
# questionsTags.write.parquet(f'{path_to_data}questionsTags_parquet')
# tags.write.parquet(f'{path_to_data}tags_parquet')
# users.write.parquet(f'{path_to_data}users_parquet')
# votes.write.parquet(f'{path_to_data}votes_parquet')
# votesTypes.write.parquet(f'{path_to_data}votesTypes_parquet')



#* Q1 - new
# answers_ordered = answers.select('owneruserid', 'creationdate').orderBy('CreationDate')
# questions_ordered = questions.select('owneruserid', 'creationdate').orderBy('CreationDate')
# comments_ordered = comments.select('userid', 'creationdate').orderBy('CreationDate')
# answers_ordered.write.parquet(f'{path_to_data}answers_creationdate_ordered')
# questions_ordered.write.parquet(f'{path_to_data}questions_creationdate_ordered')
# comments_ordered.write.parquet(f'{path_to_data}comments_creationdate_ordered')

# # MAT VIEW - interactions_ordered_parquet
# questions_selected = questions.select("owneruserid", "creationdate")
# answers_selected = answers.select("owneruserid", "creationdate")
# comments_selected = comments.select(col("userid").alias("owneruserid"), "creationdate")

# interactions = (
#     questions_selected
#     .union(answers_selected)
#     .union(comments_selected)
# ).orderBy("creationdate")

# interactions.write.parquet(f'{path_to_data}interactions_ordered_parquet')

# Adicionar uma coluna (YEAR) para depois fazer partição por essa coluna mais abrangente e utilizando-a nas queries para permitir partition pruning #*(USED)
answers = answers.withColumn('creationyear', year(answers.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
answers.write.mode('overwrite').parquet(f'{path_to_data}answers_parquet_part_year', partitionBy='creationyear')
questions = questions.withColumn('creationyear', year(questions.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
questions.write.mode('overwrite').parquet(f'{path_to_data}questions_parquet_part_year', partitionBy='creationyear')
comments = comments.withColumn('creationyear', year(comments.CreationDate)).select('UserId', 'CreationDate', 'creationyear')
comments.write.mode('overwrite').parquet(f'{path_to_data}comments_parquet_part_year', partitionBy='creationyear')

