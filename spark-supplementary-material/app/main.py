from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, lower, udf, broadcast
from pyspark.sql.functions import countDistinct, col, current_timestamp, current_date, expr, date_trunc, window, lit, coalesce, add_months
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
    #! Existe uma dessincronia com a versão do postgres

    # Calculate the date 6 months ago
    six_months_ago = add_months(current_date(), -6) #! Talvez seja esta expressão o problema, havendo alguma especie de funcionalismo diferente

    # Filter and aggregate the questions, answers, and comments dataframes
    questions_agg = questions.filter((questions["creationdate"] >= six_months_ago) & (questions["creationdate"] <= current_date())).groupBy("owneruserid").agg(count("*").alias("qcount")) #! Ver melhor o que faz o count(*)
    answers_agg = answers.filter((answers["creationdate"] >= six_months_ago) & (answers["creationdate"] <= current_date())).groupBy("owneruserid").agg(count("*").alias("acount"))
    comments_agg = comments.filter((comments["creationdate"] >= six_months_ago) & (comments["creationdate"] <= current_date())).groupBy("userid").agg(count("*").alias("ccount"))

    # DEBUG
    questions_agg.orderBy(col('qcount').desc()).show()
    answers_agg.orderBy(col('acount').desc()).show()
    comments_agg.orderBy(col('ccount').desc()).show()

    # Perform the joins
    result_df = users\
        .join(questions_agg, users["id"] == questions_agg["owneruserid"], "left") \
        .join(answers_agg, users["id"] == answers_agg["owneruserid"], "left") \
        .join(comments_agg, users["id"] == comments_agg["userid"], "left") \
        .select(col('id'), col('displayname'), (coalesce(col('qcount'), lit(0)) + coalesce(col('acount'), lit(0)) + coalesce(col('ccount'), lit(0))).alias('total'))\
        .orderBy(col('total').desc())\
        .limit(100)

    result_df.show()


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
            .config("spark.sql.adaptive.enabled", "true") \
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
    @timeit
    def w1():
        # showPartitionSize(answers) #! Existem 16 partições na answers com alguma skewness (resolver isto)
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
        q1(users, questions, answers, comments, '6 months')
        q2()
        q3()
        q4(badges, "1 minute")
    else:
        locals()[sys.argv[1]]()


if __name__ == '__main__':
    main()
