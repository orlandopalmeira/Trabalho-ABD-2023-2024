from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, lower, udf, broadcast
from pyspark.sql.functions import countDistinct, col, current_timestamp, current_date, expr, date_trunc, window, lit, coalesce, add_months, year, month, concat
# from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
import time
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

    lower_interval = current_timestamp() - expr(f"INTERVAL {interval}")

    # Filter antigo e mais simples sem utilização da nova coluna "year" #> removi o filter creationdate < now() porque não remove qualquer valor dado que não devem existir valores futuros (não sei bem se teve impacto, mas a primeira vez que testei isto depois de mudar, deu 9 secs, mas n voltou a dar)
    # questions_agg = questions.filter((questions["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("qcount"))
    # answers_agg = answers.filter((answers["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("acount"))
    # comments_agg = comments.filter((comments["creationdate"] >= lower_interval)).groupBy("userid").agg(count("*").alias("ccount"))

    # Filter and aggregate the questions, answers, and comments dataframes #> resultou numa melhoria de 11 secs para 8 secs
    lower_interval_year = year(lower_interval)
    questions_agg = questions.filter((questions["creationyear"] >= lower_interval_year) & (questions["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("qcount"))
    answers_agg = answers.filter((answers["creationyear"] >= lower_interval_year) & (answers["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("acount"))
    comments_agg = comments.filter((comments["creationyear"] >= lower_interval_year) & (comments["creationdate"] >= lower_interval)).groupBy("userid").agg(count("*").alias("ccount"))

    # Filter and aggregate the questions, answers, and comments dataframes #> comparativamente à técnica com o ano como coluna, não se nota melhorias (8.7 secs em média)
    # lower_interval_year_month = concat(year(lower_interval), lit('-'), month(lower_interval))
    # questions_agg = questions.filter((questions["creationyearmonth"] >= lower_interval_year_month) & (questions["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("qcount"))
    # answers_agg = answers.filter((answers["creationyearmonth"] >= lower_interval_year_month) & (answers["creationdate"] >= lower_interval)).groupBy("owneruserid").agg(count("*").alias("acount"))
    # comments_agg = comments.filter((comments["creationyearmonth"] >= lower_interval_year_month) & (comments["creationdate"] >= lower_interval)).groupBy("userid").agg(count("*").alias("ccount"))

    # Perform the joins
    result_df = users\
        .join(questions_agg, users["id"] == questions_agg["owneruserid"], "left") \
        .join(answers_agg, users["id"] == answers_agg["owneruserid"], "left") \
        .join(comments_agg, users["id"] == comments_agg["userid"], "left") \
        .select(col('id'), col('displayname'), (coalesce(col('qcount'), lit(0)) + coalesce(col('acount'), lit(0)) + coalesce(col('ccount'), lit(0))).alias('total'))\
        .orderBy(col('total').desc())\
        .limit(100)

    # result_df.show()
    return result_df.collect()


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
    return badges.groupBy(window(col("date"), "1 minute", startTime='2008-01-01 00:00:00')) \
          .agg(count("*").alias("count")) \
          .orderBy("window")


@timeit
def main():
    spark = SparkSession.builder \
            .master("spark://spark:7077") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            # .config("spark.executor.instances", 3) \
            # .config("spark.driver.memory", "8g") \

    data_to_path = "/app/stack/"

    ##* RepartitionByRange
    # answers = spark.read.parquet(f'{data_to_path}answers_parquet')
    # comments = spark.read.parquet(f'{data_to_path}comments_parquet')
    # questions = spark.read.parquet(f'{data_to_path}questions_parquet')
    # answers = answers.repartitionByRange('creationdate')
    # comments = comments.repartitionByRange('creationdate')
    # questions = questions.repartitionByRange('creationdate')
    # print(answers.rdd.getNumPartitions())
    ##* Year partitioning
    answers = spark.read.parquet(f'{data_to_path}answers_parquet_part_year')
    comments = spark.read.parquet(f'{data_to_path}comments_parquet_part_year')
    questions = spark.read.parquet(f'{data_to_path}questions_parquet_part_year')
    ##* Year-month partitioning
    # answers = spark.read.parquet(f'{data_to_path}answers_parquet_part_yearmonth')
    # comments = spark.read.parquet(f'{data_to_path}comments_parquet_part_yearmonth')
    # questions = spark.read.parquet(f'{data_to_path}questions_parquet_part_yearmonth')
    
    users = spark.read.parquet(f'{data_to_path}users_parquet')

    badges = spark.read.parquet(f'{data_to_path}badges_parquet')
    questionsLinks = spark.read.parquet(f'{data_to_path}questionsLinks_parquet')
    questionsTags = spark.read.parquet(f'{data_to_path}questionsTags_parquet')
    tags = spark.read.parquet(f'{data_to_path}tags_parquet') # tabela estática
    votes = spark.read.parquet(f'{data_to_path}votes_parquet')
    votesTypes = spark.read.parquet(f'{data_to_path}votesTypes_parquet') # tabela estática

    
    # Q1
    @timeit
    def w1():
        q1(users, questions, answers, comments, '6 months')

    # Q2
    @timeit
    def w2():
        q2()

    # Q3
    @timeit
    def w3():
        q3()

    # Q4
    @timeit
    def w4():
        # q4(badges, "1 minute")
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
    if len(sys.argv) < 2:
        print("Running all queries...")
        w1()
        w2()
        w3()
        w4()
    elif sys.argv[1] == "t":
        print("TEST DEBUGGING!")


    else:
        locals()[sys.argv[1]]()



if __name__ == '__main__':
    main()

    
    
    
    
    

    # tables = ['Answers', 'Badges', 'Comments', 'Questions', 'QuestionsLinks', 'QuestionsTags', 'Tags', 'Users', 'Votes', 'VotesTypes']
    # for table in tables:
    #     print(f"Loading {table}...")
    #     df = spark.read.csv(f"{data_to_path}{table}.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #     df.createOrReplaceTempView(table.lower())
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