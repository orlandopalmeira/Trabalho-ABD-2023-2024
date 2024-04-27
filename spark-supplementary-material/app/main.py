from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import col, current_timestamp, expr, lit, coalesce, year, count, broadcast, avg, asc, desc, window
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

    # questions_selected = questions.select("owneruserid", "creationdate")
    # answers_selected = answers.select("owneruserid", "creationdate")
    questions_selected = questions
    answers_selected = answers
    comments_selected = comments.select(col("userid").alias("owneruserid"), "creationdate")

    lower_interval = current_timestamp() - expr(f"INTERVAL {interval}")

    interactions = (
        questions_selected
        .union(answers_selected)
        .union(comments_selected)
        .filter(col("creationdate").between(lower_interval, current_timestamp()))
        .groupBy("owneruserid")
        .agg(count("*").alias("interaction_count"))
    )

    result_df = (
        users
        .join(broadcast(interactions), users["id"] == interactions["owneruserid"], "left")
        .select(
            users["id"],
            users["displayname"],
            coalesce(interactions["interaction_count"], lit(0)).cast(IntegerType()).alias("total")
        )
        .orderBy(col("total").desc())
        .limit(100)
    )
    
    return result_df.collect()


@timeit
def q1_year(users: DataFrame, questions: DataFrame, answers: DataFrame, comments: DataFrame, interval: StringType = "6 months") -> List[Row]:
    
    # questions_selected = questions.select("owneruserid", "creationdate")
    # answers_selected = answers.select("owneruserid", "creationdate")
    # comments_selected = comments.select(col("userid").alias("owneruserid"), "creationdate")

    #> Versão com a coluna creation_year
    questions_selected = questions.select("owneruserid", "creationdate", "creationyear")
    answers_selected = answers.select("owneruserid", "creationdate", "creationyear")
    comments_selected = comments.select(col("userid").alias("owneruserid"), "creationdate", "creationyear")

    lower_interval = current_timestamp() - expr(f"INTERVAL {interval}")
    lower_interval_year = year(lower_interval)

    interactions = (
        questions_selected
        .union(answers_selected)
        .union(comments_selected)
        # .filter(col("creationdate").between(lower_interval, current_timestamp()))
        .filter((col("creationyear") >= lower_interval_year) & (col("creationdate").between(lower_interval, current_timestamp()))) #> Versão com a coluna creation_year
        .groupBy("owneruserid")
        .agg(count("*").alias("interaction_count"))
    )

    result_df = (
        users
        .join(broadcast(interactions), users["id"] == interactions["owneruserid"], "left")
        .select(
            users["id"],
            users["displayname"],
            coalesce(interactions["interaction_count"], lit(0)).cast(IntegerType()).alias("total")
        )
        .orderBy(col("total").desc())
        .limit(100)
    )
    
    return result_df.collect()


@timeit
def q1_new(users: DataFrame, interactions: DataFrame, interval: StringType = "6 months"):
    lower_interval = current_timestamp() - expr(f"INTERVAL {interval}")
    # lower_interval_year = year(lower_interval)

    interactions_grouped = (
        # interactions.filter((col("creationyear") >= lower_interval_year) & (col("creationdate").between(lower_interval, current_timestamp()))) #> Versão com a coluna creation_year
        interactions.filter((col("creationdate").between(lower_interval, current_timestamp())))
        .groupBy("owneruserid")
        .agg(count("*").alias("interaction_count"))
    )

    result_df = (
        users
        .join(broadcast(interactions_grouped), users["id"] == interactions["owneruserid"], "left")
        .select(
            users["id"],
            users["displayname"],
            coalesce(interactions_grouped["interaction_count"], lit(0)).cast(IntegerType()).alias("total")
        )
        .orderBy(col("total").desc())
        .limit(100)
    )
    
    return result_df.collect()

@timeit
# def q2(users: DataFrame, answers: DataFrame, votes: DataFrame, votesTypes: DataFrame, interval: StringType = "5 years", bucketInterval : IntegerType = 5000):
def q2():
    pass

@timeit
def q3(tags: DataFrame, questionsTags: DataFrame, answers: DataFrame, inferiorLimit: IntegerType = 10):

# CREATE MATERIALIZED VIEW TagQuestionCounts AS
# SELECT qt.tagid, qt.questionid, COUNT(*) AS total
# FROM questionstags qt
# LEFT JOIN answers a ON a.parentid = qt.questionid
# GROUP BY qt.tagid, qt.questionid
# 
# WITH FilteredTags AS (
#     SELECT tagid
#     FROM TagQuestionCounts
#     GROUP BY tagid
#     HAVING COUNT(*) > 10
# )
# 
# SELECT t.tagname, ROUND(AVG(tqc.total), 3) AS avg_total, COUNT(*) AS tag_count
# FROM TagQuestionCounts tqc
# JOIN FilteredTags ft ON ft.tagid = tqc.tagid
# LEFT JOIN tags t ON t.id = tqc.tagid
# GROUP BY t.tagname
# ORDER BY avg_total DESC, tag_count DESC, t.tagname;

    # spark = SparkSession.builder \
    # .appName("MaterializedViewUsage") \
    # .getOrCreate()

    tag_question_counts = questionsTags.alias("qt") \
                             .join(answers.alias("a"), questionsTags["questionid"] == answers["parentid"], "left") \
                             .groupBy("qt.tagid", "qt.questionid") \
                             .agg(count("*").alias("total"))

    tag_question_counts.write.mode("overwrite").parquet("stack/tag_question_counts.parquet")

    # Read the materialized view from the file
    # tag_question_counts = spark.read.parquet("tag_question_counts.parquet")
    
    # Create the FilteredTags view
    filtered_tags = tag_question_counts.groupBy("tagid").agg(count("*").alias("count")) \
                                       .filter("count > 10") \
                                       .select("tagid")
    
    # Perform the final aggregation
    result_df = tag_question_counts.alias("tqc") \
                                   .join(filtered_tags.alias("ft"), "tagid", "inner") \
                                   .join(tags.alias("t"), tag_question_counts["tagid"] == tags["id"], "left") \
                                   .groupBy("t.tagname") \
                                   .agg(spark_round(avg("tqc.total"), 3).alias("avg_total"), count("*").alias("tag_count")) \
                                   .orderBy("avg_total", "tag_count", "t.tagname").sort(desc("avg_total"), desc("tag_count"), asc("t.tagname"))

    result_df.write.csv("stack/res-q3.csv")

    return result_df.collect()

@timeit
def q3_mv(mat_view: DataFrame, inferiorLimit: IntegerType = 10):
    # q3 com vista materializada
    result = (
        mat_view.select(
            "tagname", 
            col("round(avg(total), 3)").alias("round"), 
            col("count(1)").alias("count")
        )
        .filter(col("tag_count") > inferiorLimit)
        .orderBy(col("round").desc(), col("count").desc(), "tagname")
    )
    
    return result.collect()

@timeit
def q4(badges: DataFrame, bucketWindow: StringType = "1 minute"):
    return badges.groupBy(window(col("date"), "1 minute", startTime='2008-01-01 00:00:00')) \
          .agg(count("*").alias("count")) \
          .orderBy("window")


def main():
    spark = SparkSession.builder \
            .master("spark://spark:7077") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            # .config("spark.driver.memory", "4g") \
            # .config("spark.executor.cores", "2") \
            # .config("spark.executor.instances", 3) \

    data_to_path = "/app/stack/"

    
    # Q1
    @timeit
    def w1():
        # Reads
        users = spark.read.parquet(f"{data_to_path}users_parquet")
        questions = spark.read.parquet(f"{data_to_path}questions_creationdate_ordered")
        answers = spark.read.parquet(f"{data_to_path}answers_creationdate_ordered")
        comments = spark.read.parquet(f"{data_to_path}comments_creationdate_ordered")
        
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')

    @timeit
    def w1_year():
        # Reads
        users = spark.read.parquet(f"{data_to_path}users_parquet")
        questions = spark.read.parquet(f"{data_to_path}questions_parquet_part_year")
        answers = spark.read.parquet(f"{data_to_path}answers_parquet_part_year")
        comments = spark.read.parquet(f"{data_to_path}comments_parquet_part_year")

        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')


    @timeit
    def w1_new():
        # Reads
        interactions = spark.read.parquet(f"{data_to_path}interactions_ordered_parquet")
        users = spark.read.parquet(f"{data_to_path}users_parquet")
        
        q1_new(users, interactions, '6 months')
        q1_new(users, interactions, '6 months')
        q1_new(users, interactions, '6 months')
        q1_new(users, interactions, '6 months')
        q1_new(users, interactions, '6 months')

    # Q2
    @timeit
    def w2():
        q2()

    # Q3
    @timeit
    def w3():
        # Reads
        tags = spark.read.parquet(f'{data_to_path}tags_parquet') # tabela estática
        questionsTags = spark.read.parquet(f'{data_to_path}questionsTags_parquet')
        answers = spark.read.parquet(f'{data_to_path}answers_parquet')

        q3(tags, questionsTags, answers, 10)

    
    @timeit
    def w3_mv():
        # # para criar a vista materializada em ficheiro
        # questionsTags.createOrReplaceTempView("questionstags")
        # answers.createOrReplaceTempView("answers")
        # tags.createOrReplaceTempView("tags")
        # spark.sql("""
        #     SELECT qt.tagid, t.tagname, qt.questionid, COUNT(*) AS total
        #     FROM questionstags qt
        #     LEFT JOIN answers a ON a.parentid = qt.questionid
        #     LEFT JOIN tags t ON t.id = qt.tagid
        #     GROUP BY qt.tagid, qt.questionid, t.tagname
        # """).createOrReplaceTempView("TagQuestionCounts")
        # spark.sql("""
        #     SELECT tagid, COUNT(*) as tag_count
        #     FROM TagQuestionCounts
        #     GROUP BY tagid
        # """).createOrReplaceTempView("FilteredTags")
        # result = spark.sql("""
        #     SELECT tqc.tagname, ROUND(AVG(tqc.total), 3), ft.tag_count, COUNT(*)
        #     FROM TagQuestionCounts tqc
        #     JOIN FilteredTags ft ON ft.tagid = tqc.tagid
        #     GROUP BY tqc.tagname, ft.tag_count"""
        # )
        # result.write.parquet("mv_q3.parquet")

        mat_view_q3 = spark.read.parquet("mv_q3.parquet")

        for i in range(100):
            q3_mv(mat_view_q3, 10)

    # Q4
    @timeit
    def w4():
        badges = spark.read.parquet(f'{data_to_path}badges_parquet')
        
        # TODO METER ESTA FILTERED_BADGES EM FICHEIRO
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
    


    if len(sys.argv) < 2:
        print("Running all queries...")
        w1()
        w2()
        w3()
        w4()
    elif sys.argv[1] == "t":
        print("TEST DEBUGGING!")
        print("W1")
        w1()
        print("W1_YEAR")
        w1_year()
        print("W1_NEW")
        w1_new()


    else:
        locals()[sys.argv[1]]()


if __name__ == '__main__':
    main()

    