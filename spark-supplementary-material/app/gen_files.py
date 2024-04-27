import sys
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



#* Q1
Q1_PATH = f"{path_to_data}Q1/"

def q1_users():
    new_users = users.select("id", "displayname")
    new_users.write.parquet(f'{Q1_PATH}users_id_displayname')

def q1_ans_questions_comments_ord():
    answers_ordered = answers.select('owneruserid', 'creationdate').orderBy('CreationDate')
    questions_ordered = questions.select('owneruserid', 'creationdate').orderBy('CreationDate')
    comments_ordered = comments.select(col('userid').alias('owneruserid'), 'creationdate').orderBy('CreationDate')
    answers_ordered.write.parquet(f'{Q1_PATH}answers_creationdate_ordered')
    questions_ordered.write.parquet(f'{Q1_PATH}questions_creationdate_ordered')
    comments_ordered.write.parquet(f'{Q1_PATH}comments_creationdate_ordered')

# MAT VIEW - interactions_ordered_parquet
def q1_interactions_ordered_parquet():
    questions_selected = questions.select("owneruserid", "creationdate")
    answers_selected = answers.select("owneruserid", "creationdate")
    comments_selected = comments.select(col("userid").alias("owneruserid"), "creationdate")

    interactions = (
        questions_selected
        .union(answers_selected)
        .union(comments_selected)
    ).orderBy("creationdate")

    interactions.write.parquet(f'{Q1_PATH}interactions_ordered_parquet')

# Adicionar uma coluna (YEAR) para depois fazer partição por essa coluna mais abrangente e utilizando-a nas queries para permitir partition pruning
def q1_add_year_partition():
    new_answers = answers.withColumn('creationyear', year(answers.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
    new_answers.write.parquet(f'{Q1_PATH}answers_parquet_part_year', partitionBy='creationyear')
    new_questions = questions.withColumn('creationyear', year(questions.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
    new_questions.write.parquet(f'{Q1_PATH}questions_parquet_part_year', partitionBy='creationyear')
    new_comments = comments.withColumn('creationyear', year(comments.CreationDate)).select('UserId', 'CreationDate', 'creationyear')
    new_comments.write.parquet(f'{Q1_PATH}comments_parquet_part_year', partitionBy='creationyear')


def q1_repartitionByRange():
    answers_rep = answers.select('owneruserid', 'creationdate').repartitionByRange(col('creationdate'))
    questions_rep = questions.select('owneruserid', 'creationdate').repartitionByRange(col('creationdate'))
    comments_rep = comments.select(col('userid').alias('owneruserid'), 'creationdate').repartitionByRange(col('creationdate'))
    answers_rep.write.parquet(f'{Q1_PATH}answers_creationdate_reprange')
    questions_rep.write.parquet(f'{Q1_PATH}questions_creationdate_reprange')
    comments_rep.write.parquet(f'{Q1_PATH}comments_creationdate_reprange')


#* Q2
Q2_PATH = f"{path_to_data}Q2/"



#* Q3
Q3_PATH = f"{path_to_data}Q3/"



#* Q4
Q4_PATH = f"{path_to_data}Q4/"



if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Running pre-defined function...")
        # q1_users()
        q1_add_year_partition()
        q1_interactions_ordered_parquet()


    else:
        print("Running custom function...")

        locals()[sys.argv[1]]()