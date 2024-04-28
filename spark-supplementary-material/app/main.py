from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import col, current_timestamp, expr, lit, coalesce, year, count, broadcast, avg, asc, desc, window
# from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
import time
from functools import wraps
import sys

data_to_path = "/app/stack/"
Q1_PATH = f"{data_to_path}Q1/"
Q2_PATH = f"{data_to_path}Q2/"
Q3_PATH = f"{data_to_path}Q3/"
Q4_PATH = f"{data_to_path}Q4/"

times = {}

# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        measured_time = round(time.time() - t, 3)
        if times.get(f.__name__):
            times[f.__name__].append(measured_time)
        else:
            times[f.__name__] = [measured_time]
        print(f'{f.__name__}: {measured_time}s')
        return result
    return wrap

def count_rows(iterator):
    yield len(list(iterator))

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')

def write_result(res, filename):
    with open(filename, "w") as f:
        for row in res:
            f.write(str(row) + "\n")

# Queries analíticas

@timeit
def q1(users: DataFrame, questions: DataFrame, answers: DataFrame, comments: DataFrame, interval: StringType = "6 months") -> List[Row]:

    questions_selected = questions
    answers_selected = answers
    comments_selected = comments#.select(col("userid").alias("owneruserid"), "creationdate")

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
def q1_interactions_mv(users: DataFrame, interactions: DataFrame, interval: StringType = "6 months"):
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

    # result_df.write.csv("stack/res-q3.csv")

    return result_df.collect()

@timeit
def q3_mv(mat_view: DataFrame, inferiorLimit: IntegerType = 10):
    # q3 com vista materializada
    result = mat_view.filter(col("count") > inferiorLimit)
    # result.write.csv("stack/res-q3-MV.csv")
    return result.collect()

@timeit
def q4(badges: DataFrame, bucketWindow: StringType = "1 minute"):
    result = badges.groupBy(window(col("date"), "1 minute")) \
          .agg(count("*").alias("count")) \
          .orderBy("window")
    
    return result.collect()


def main():
    spark = SparkSession.builder \
            .master("spark://spark:7077") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.executor.instances", 3) \
            .config("spark.driver.memory", "8g") \
            .getOrCreate()
            # .config("spark.executor.cores", "2") \
            # .config("spark.executor.memory", "1g") \



    # Q1
    def w1():
        # Reads
        users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
        questions = spark.read.parquet(f"{Q1_PATH}questions_creationdate_ordered")
        answers = spark.read.parquet(f"{Q1_PATH}answers_creationdate_ordered")
        comments = spark.read.parquet(f"{Q1_PATH}comments_creationdate_ordered")
        
        res = q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')

        # write_result(res, "w1.csv")

    def w1_year():
        # Reads
        users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
        questions = spark.read.parquet(f"{Q1_PATH}questions_parquet_part_year")
        answers = spark.read.parquet(f"{Q1_PATH}answers_parquet_part_year")
        comments = spark.read.parquet(f"{Q1_PATH}comments_parquet_part_year")

        res=q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')
        q1_year(users, questions, answers, comments, '6 months')

        # write_result(res, "w1-year.csv")


    def w1_int_mv():
        # Reads
        users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
        interactions = spark.read.parquet(f"{Q1_PATH}interactions_ordered_parquet")
        
        res=q1_interactions_mv(users, interactions, '6 months')
        q1_interactions_mv(users, interactions, '6 months')
        q1_interactions_mv(users, interactions, '6 months')
        q1_interactions_mv(users, interactions, '6 months')
        q1_interactions_mv(users, interactions, '6 months')

        # write_result(res, "w1-int-mv.csv")


    def w1_range():
        """Using RepartitionByRange('creationdate') files"""
        # Reads
        users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
        questions = spark.read.parquet(f"{Q1_PATH}questions_creationdate_reprange")
        answers = spark.read.parquet(f"{Q1_PATH}answers_creationdate_reprange")
        comments = spark.read.parquet(f"{Q1_PATH}comments_creationdate_reprange")
        
        res=q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')

        # write_result(res, "w1-range.csv")

    def w1_zip():
        # Reads
        users = spark.read.parquet(f"{Q1_PATH}users_id_displayname_zip")
        questions = spark.read.parquet(f"{Q1_PATH}questions_creationdate_zip")
        answers = spark.read.parquet(f"{Q1_PATH}answers_creationdate_zip")
        comments = spark.read.parquet(f"{Q1_PATH}comments_creationdate_zip")
        
        res=q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')
        q1(users, questions, answers, comments, '6 months')

        # write_result(res, "w1-zip.csv")

    # Q2
    def w2():
        q2()

    # Q3
    def w3():
        # Reads
        tags = spark.read.parquet(f'{Q3_PATH}tags_parquet') # tabela estática
        questionsTags = spark.read.parquet(f'{Q3_PATH}questionsTags_parquet')
        answers = spark.read.parquet(f'{Q3_PATH}answers_parquet')

        for i in range(1):
            q3(tags, questionsTags, answers, 10)

    
    def w3_mv():
        mat_view_q3 = spark.read.parquet(f"{Q3_PATH}mv_q3.parquet")

        for i in range(1):
            q3_mv(mat_view_q3, 10)

    # Q4
    def w4():
        mv_badges = spark.read.parquet(f"{Q4_PATH}badges_mat_view_ord")
        # mv_badges = spark.read.parquet(f"{Q4_PATH}badges_mat_view")
        q4(mv_badges, "1 minute")
        q4(mv_badges, "1 minute")
        q4(mv_badges, "1 minute")
        q4(mv_badges, "1 minute")
    


    if len(sys.argv) < 2:
        print("Running all queries...")
        w1()
        w2()
        w3()
        w4()
    elif sys.argv[1] == "t":
        pass

    else:
        locals()[sys.argv[1]]()
    
    # Calculating average times
    for func in times:
        avg_time = sum(times[func]) / len(times[func])
        avg_time = round(avg_time, 3)
        print(f'Avg of {func}: {avg_time} seconds')



if __name__ == '__main__':
    main()

    