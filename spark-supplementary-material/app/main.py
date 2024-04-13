from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, lower, udf, broadcast
from pyspark.sql.functions import countDistinct, col, current_timestamp, current_date, expr, date_trunc, window
# from pyspark.sql.window import Window
# from pyspark.sql.functions import count, spark_partition_id, lower, udf, broadcast, date_sub, current_date, current_timestamp, col, collect_list, floor, year, avg, rank, expr
from pyspark.sql.types import StringType, IntegerType
import time
# from datetime import datetime, timedelta
from functools import wraps
import sys



# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        print(f'{f.__name__}: {round(time.time() - t, 3)}s')
        return result
    return wrap

def count_rows(iterator):
    yield len(list(iterator))

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')


# Queries analíticas
@timeit
def q1(users: DataFrame, questions: DataFrame, answers: DataFrame, comments: DataFrame, interval: StringType = "6 months") -> List[Row]:

    filtered_questions = questions.where(questions.CreationDate.between(current_timestamp() - expr(f"INTERVAL {interval}"), current_timestamp()))
    filtered_answers = answers.where(answers.CreationDate.between(current_timestamp() - expr(f"INTERVAL {interval}"), current_timestamp()))
    filtered_comments = comments.where(comments.CreationDate.between(current_timestamp() - expr(f"INTERVAL {interval}"), current_timestamp()))

    joined = users.join(filtered_questions, users["id"] == filtered_questions["owneruserid"], "left") \
                        .join(filtered_answers, users["id"] == filtered_answers["owneruserid"], "left") \
                        .join(filtered_comments, users["id"] == filtered_comments["userid"], "left")

    result = joined.groupBy(users["id"], users["displayname"]) \
                         .agg((countDistinct(filtered_questions["id"]) + \
                               countDistinct(filtered_answers["id"]) + \
                               countDistinct(filtered_comments["id"])).alias("total")) \
                         .orderBy(col("total").desc())

    return result.show(100)

@timeit
# def q2(users: DataFrame, answers: DataFrame, votes: DataFrame, votesTypes: DataFrame, interval: StringType = "5 years", bucketInterval : IntegerType = 5000):
def q2():
    pass

@timeit
# def q3(tags: DataFrame, questionsTags: DataFrame, answers: DataFrame, inferiorLimit: IntegerType = 10):
def q3():
    pass

@timeit
def q4(badges: DataFrame, bucketWindow: StringType = "1 minute"):
    pass
    # return badges.groupBy(window(col("date"), "1 minute", startTime='2008-01-01 00:00:00')) \
    #       .agg(count("*").alias("count")) \
    #       .orderBy("window")



def main():
    spark = SparkSession.builder \
            .master("spark://spark:7077") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()


    data_to_path = "/app/stack/"
    # tables = ['Answers', 'Badges', 'Comments', 'Questions', 'QuestionsLinks', 'QuestionsTags', 'Tags', 'Users', 'Votes', 'VotesTypes']
    # for table in tables:
    #     print(f"Loading {table}...")
    #     df = spark.read.csv(f"{data_to_path}{table}.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #     df.createOrReplaceTempView(table.lower())

    # Loading parquet files
    print("Loading parquet data...")
    answers = spark.read.parquet(f'{data_to_path}answers_parquet')
    badges = spark.read.parquet(f'{data_to_path}badges_parquet')
    comments = spark.read.parquet(f'{data_to_path}comments_parquet')
    questions = spark.read.parquet(f'{data_to_path}questions_parquet')
    questionsLinks = spark.read.parquet(f'{data_to_path}questionsLinks_parquet')
    questionsTags = spark.read.parquet(f'{data_to_path}questionsTags_parquet')
    tags = spark.read.parquet(f'{data_to_path}tags_parquet') # tabela estática
    users = spark.read.parquet(f'{data_to_path}users_parquet')
    votes = spark.read.parquet(f'{data_to_path}votes_parquet')
    votesTypes = spark.read.parquet(f'{data_to_path}votesTypes_parquet') # tabela estática
    print("Parquet data loaded.")

    # Loading csv files
    # print("Loading csv data...")
    # answers_csv = spark.read.csv(f"{data_to_path}Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # badges_csv = spark.read.csv(f"{data_to_path}Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # comments_csv = spark.read.csv(f"{data_to_path}Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # questions_csv = spark.read.csv(f"{data_to_path}Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # questionsLinks_csv = spark.read.csv(f"{data_to_path}QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # questionsTags_csv = spark.read.csv(f"{data_to_path}QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # tags_csv = spark.read.csv(f"{data_to_path}Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # users_csv = spark.read.csv(f"{data_to_path}Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # votes_csv = spark.read.csv(f"{data_to_path}Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # votesTypes_csv = spark.read.csv(f"{data_to_path}VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    # print("CSV data loaded.")

    # Check functioning
    # questions.sample(0.01).show(5)


    # Q1
    q1(users, questions, answers, comments, '6 months')


    # Q4
    filtered_badges = badges.filter(
        (col("tagbased") == False) &
        (~col("name").isin(
            'Analytical',
            'Census',
            'Documentation Beta',
            'Documentation Pioneer',
            'Documentation User',
            'Reversal',
            'Tumbleweed'
        )) &
        (col("class").isin(1, 2, 3)) &
        (col("userid") != -1)
    ).select("date").cache()
    
    q4(filtered_badges, "1 minute")


    ## Maneira dinamica de chamar as queries (mas ainda tenho de ver como funciona caches e assim neste caso. Talvez precise de englobar as queries em workloads)
    # if len(sys.argv) < 2:
    #     print("Running all queries...")
    #     q1(users, questions, answers, comments, '6 months')
    #     q2()
    #     q3()
    #     q4(badges, "1 minute")
    #     return
    # else:
    #     locals()[sys.argv[1]]()


if __name__ == '__main__':
    main()
