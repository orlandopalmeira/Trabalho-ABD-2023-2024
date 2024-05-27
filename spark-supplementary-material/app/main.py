from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import col, current_timestamp, expr, lit, coalesce, year, count, broadcast, avg, asc, desc, window, greatest, sequence, explode, date_sub, current_timestamp, floor
# from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

from functools import wraps
import time, sys, re

# from q1 import *
# from q2 import *
# from q3 import *
# from q4 import *

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
        key = f"{f.__name__}_{args[-1]}"
        if "q2" in f.__name__:
            key = f"{f.__name__}_{args[-2]}_{args[-1]}"
        if times.get(key, None) is not None:
            times[key].append(measured_time)
        else:
            # times[key] = [measured_time]
            times[key] = [] #? para caso queira ignorar a primeira medição
        # print(f'{key}: {measured_time}s')
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
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#******************************** QUERY 1 ********************************

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

    # print(questions_selected.rdd.getNumPartitions())
    # print(answers_selected.rdd.getNumPartitions())
    # print(comments_selected.rdd.getNumPartitions())

    lower_interval = current_timestamp() - expr(f"INTERVAL {interval}")
    lower_interval_year = year(lower_interval)

    interactions = (
        questions_selected
        .union(answers_selected)
        .union(comments_selected)
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
    # result_df.show()
    return result_df.collect()




#******************************** WORKLOAD 1 ********************************
# Q1
def w1():
    # Reads
    users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
    questions = spark.read.parquet(f"{Q1_PATH}questions_creationdate_ordered")
    answers = spark.read.parquet(f"{Q1_PATH}answers_creationdate_ordered")
    comments = spark.read.parquet(f"{Q1_PATH}comments_creationdate_ordered")
    
    reps = 6
    for _ in range(reps):
        q1(users, questions, answers, comments, '1 months')
    for _ in range(reps):
        q1(users, questions, answers, comments, '3 months')
    for _ in range(reps):
        q1(users, questions, answers, comments, '6 months')
    for _ in range(reps):
        q1(users, questions, answers, comments, '1 year')
    for _ in range(reps):
        q1(users, questions, answers, comments, '2 year')

    # write_result(res, "w1.csv")

def w1_year():
    # Reads
    users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
    questions = spark.read.parquet(f"{Q1_PATH}questions_parquet_part_year")
    answers = spark.read.parquet(f"{Q1_PATH}answers_parquet_part_year")
    comments = spark.read.parquet(f"{Q1_PATH}comments_parquet_part_year")

    reps = 6
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '1 month')
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '3 month')
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '6 months')
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '1 year')
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '2 year')

    # write_result(res, "w1-year.csv")


def w1_range():
    """Using RepartitionByRange('creationdate') files"""
    # Reads
    users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
    questions = spark.read.parquet(f"{Q1_PATH}questions_creationdate_reprange")
    answers = spark.read.parquet(f"{Q1_PATH}answers_creationdate_reprange")
    comments = spark.read.parquet(f"{Q1_PATH}comments_creationdate_reprange")
    
    reps = 6
    for _ in range(reps):
        q1(users, questions, answers, comments, '1 month')
    for _ in range(reps):
        q1(users, questions, answers, comments, '3 month')
    for _ in range(reps):
        q1(users, questions, answers, comments, '6 months')
    for _ in range(reps):
        q1(users, questions, answers, comments, '1 year')
    for _ in range(reps):
        q1(users, questions, answers, comments, '2 year')

    # write_result(res, "w1-range.csv")

def w1_final():
    # Reads
    users = spark.read.parquet(f"{Q1_PATH}users_id_displayname")
    questions = spark.read.parquet(f"{Q1_PATH}questions_parquet_part_year")
    answers = spark.read.parquet(f"{Q1_PATH}answers_parquet_part_year")
    comments = spark.read.parquet(f"{Q1_PATH}comments_parquet_part_year")

    reps = 6
    for _ in range(reps):
        q1_year(users, questions, answers, comments, '6 months')

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#******************************** QUERY 2 ********************************

def is_leap_year(year):
    """
    Verifica se o ano é bissexto.
    """
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def parse_interval_3(interval):
    """
    Função para extrair o número e unidade do intervalo e calcular o número de dias correspondente de forma precisa.
    """
    # Use expressão regular para extrair número e unidade do intervalo
    match = re.match(r"(\d+)\s+(\w+)", interval)
    if match:
        number = int(match.group(1))
        unit = match.group(2).lower()

        if unit.startswith("year"):
            # Calcular o número exato de dias em 'number' anos considerando anos bissextos
            days = sum(366 if is_leap_year(year) else 365 for year in range(1, number + 1))
        elif unit.startswith("month"):
            # Calcular o número exato de dias em 'number' meses
            days_in_month = [31, 29 if is_leap_year(year) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            days = sum(days_in_month[:number])
        elif unit.startswith("week"):
            # Calcular o número exato de dias em 'number' semanas
            days = number * 7
        elif unit.startswith("day"):
            # 'number' dias
            days = number
        else:
            raise ValueError(f"Unidade de intervalo não suportada: {unit}")

        return days
    else:
        raise ValueError(f"Formato de intervalo inválido: {interval}")

@timeit
def q2(u: DataFrame, year_range: DataFrame,  max_reputation_per_year: DataFrame, interval: StringType = "5 years", bucketInterval: IntegerType = 5000):

    # SELECT yr.year, generate_series(0, GREATEST(mr.max_rep,0), 5000) AS reputation_range
    # FROM year_range yr
    # LEFT JOIN max_reputation_per_year mr ON yr.year = mr.year

    # juntar os dados de yr e mr
    buckets = year_range.alias("yr").join(max_reputation_per_year.alias("mr"), year_range["year"] == max_reputation_per_year["year"], "left")

    # gerar a coluna reputation_range
    buckets = buckets.withColumn(
                    "reputation_range",
                    explode(sequence(lit(0), greatest(col("mr.max_rep"), lit(0)), lit(bucketInterval)))
                )

    buckets = buckets.select(col("yr.year").alias("year"), col("reputation_range"))

    u = u.where(col("votes_creationdate") >= date_sub(current_timestamp(), parse_interval_3(interval))).select('id','creationdate','reputation').distinct()
    u = u.withColumn("u_year", year(u["creationdate"]))
    u = u.withColumn("reputation_range", floor(u["reputation"] / bucketInterval) * bucketInterval)
    u = u.withColumnRenamed("reputation_range", "u_rg")

    joined_df = buckets.join(u,
                         (buckets["year"] == u["u_year"]) &
                         (buckets["reputation_range"] == u["u_rg"]),
                         "left")

    result_df = joined_df.groupBy("year", "reputation_range") \
                     .agg({"id": "count"}) \
                     .withColumnRenamed("count(id)", "total")

    sorted_result_df = result_df.orderBy("year", "reputation_range")

    return sorted_result_df.collect()


#******************************** WORKLOAD 2 ********************************

# Q2
def w2():
    # Reads
    year_range = spark.read.parquet(f"{Q2_PATH}year_range")
    max_reputation_per_year = spark.read.parquet(f"{Q2_PATH}max_reputation_per_year")
    u = spark.read.parquet(f"{Q2_PATH}u")

    reps=6
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "1 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "3 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "5 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "7 year", 5000)


def w2_ord():

    # Reads
    year_range = spark.read.parquet(f"{Q2_PATH}year_range")
    max_reputation_per_year = spark.read.parquet(f"{Q2_PATH}max_reputation_per_year")
    u = spark.read.parquet(f"{Q2_PATH}u_ord")

    reps=6
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "1 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "3 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "5 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "7 year", 5000)

    #write_result(res, "w2.csv")

def w2_ord_part():

    # Reads
    year_range = spark.read.parquet(f"{Q2_PATH}year_range")
    max_reputation_per_year = spark.read.parquet(f"{Q2_PATH}max_reputation_per_year")
    u = spark.read.parquet(f"{Q2_PATH}u_ord_part")

    reps=6
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "1 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "3 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "5 year", 5000)
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "7 year", 5000)

def w2_final():
    # Reads
    year_range = spark.read.parquet(f"{Q2_PATH}year_range")
    max_reputation_per_year = spark.read.parquet(f"{Q2_PATH}max_reputation_per_year")
    u = spark.read.parquet(f"{Q2_PATH}u")

    reps=6
    for _ in range(reps):
        q2(u, year_range, max_reputation_per_year, "5 year", 5000)


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#******************************** QUERY 3 ********************************

@timeit
def q3(tags: DataFrame, questionstags: DataFrame, answers: DataFrame, inferiorLimit: IntegerType = 10):
    questionstags.createOrReplaceTempView("questionstags")
    tags.createOrReplaceTempView("tags")
    answers.createOrReplaceTempView("answers")
    result = spark.sql(f"""
        SELECT tagname, round(avg(total), 3) AS avg_total, count(*) AS count_total
        FROM (
            SELECT t.tagname, qt.questionid, count(*) AS total
            FROM tags t
            JOIN questionstags qt ON qt.tagid = t.id
            LEFT JOIN answers a ON a.parentid = qt.questionid
            WHERE t.id IN (
                SELECT tagid
                FROM questionstags
                GROUP BY tagid
                HAVING count(*) > {inferiorLimit}
            )
            GROUP BY t.tagname, qt.questionid
        ) AS subquery
        GROUP BY tagname
        ORDER BY avg_total DESC, count_total DESC, tagname
    """)
    # result.show()
    return result.collect()

@timeit
def q3_mv(mat_view: DataFrame, inferiorLimit: IntegerType = 10):
    result = mat_view.filter(col("count") > inferiorLimit)
    return result.collect()

#******************************** WORKLOAD 3 ********************************
def w3_base():
    # Reads
    tags = spark.read.parquet(f'{Q3_PATH}tags_parquet') # tabela estática
    questionsTags = spark.read.parquet(f'{Q3_PATH}questionsTags_parquet')
    answers = spark.read.parquet(f'{Q3_PATH}answers_parquet')
    reps=3
    for _ in range(reps):
        q3(tags, questionsTags, answers, 10)
        q3(tags, questionsTags, answers, 30)
        q3(tags, questionsTags, answers, 50)


def w3_mv():
    mat_view_q3 = spark.read.parquet(f"{Q3_PATH}mv_parquet")
    # q3_mv(mat_view_q3, 50)
    reps=6
    for _ in range(reps):
        q3_mv(mat_view_q3, 10)
    for _ in range(reps):
        q3_mv(mat_view_q3, 30)
    for _ in range(reps):
        q3_mv(mat_view_q3, 50)
    for _ in range(reps):
        q3_mv(mat_view_q3, 100)
    

def w3_mv_ord():
    mat_view_q3 = spark.read.parquet(f"{Q3_PATH}mv_parquet_ord")
    # q3_mv(mat_view_q3, 50)
    reps=6
    for _ in range(reps):
        q3_mv(mat_view_q3, 10)
    for _ in range(reps):
        q3_mv(mat_view_q3, 30)
    for _ in range(reps):
        q3_mv(mat_view_q3, 50)
    for _ in range(reps):
        q3_mv(mat_view_q3, 100)


def w3_final():
    mat_view_q3 = spark.read.parquet(f"{Q3_PATH}mv_parquet")
    reps=6
    for _ in range(reps):
        q3_mv(mat_view_q3, 10)


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#******************************** QUERY 4 ********************************
@timeit
def q4(badges: DataFrame, bucketWindow: StringType = "1 minute"):
    result = badges.groupBy(window(col("date"), bucketWindow)) \
          .agg(count("*").alias("count")) \
          .orderBy("window")
    return result.collect()


#******************************** WORKLOAD 4 ********************************
def w4():
    mv_badges = spark.read.parquet(f"{Q4_PATH}badges_mat_view")
    reps = 3
    for _ in range(reps):
        q4(mv_badges, "1 minute")
    for _ in range(reps):
        q4(mv_badges, "10 minute")
    for _ in range(reps):
        q4(mv_badges, "30 minutes")
    for _ in range(reps):
        q4(mv_badges, "2 hour")
    for _ in range(reps):
        q4(mv_badges, "6 hour")

def w4_ord():
    mv_badges = spark.read.parquet(f"{Q4_PATH}badges_mat_view_ord")
    reps = 3
    for _ in range(reps):
        q4(mv_badges, "1 minute")
    for _ in range(reps):
        q4(mv_badges, "10 minute")
    for _ in range(reps):
        q4(mv_badges, "30 minutes")
    for _ in range(reps):
        q4(mv_badges, "2 hour")
    for _ in range(reps):
        q4(mv_badges, "6 hour")

def w4_final():
    mv_badges = spark.read.parquet(f"{Q4_PATH}badges_mat_view_ord")
    reps = 3
    for _ in range(reps):
        q4(mv_badges, "1 minute")


#?############################ SPARK SESSION ####################################


spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.instances", 3) \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()
    # .config("spark.driver.memory", "16g") \

    # .config("spark.executor.cores", "2") \
    # .config("spark.executor.memory", "1g") \



if len(sys.argv) < 2:
    print("Running all queries...")
    w1_final()
    w2_final()
    w3_final()
    w4_final()
elif sys.argv[1] == "t":
    pass
else:
    locals()[sys.argv[1]]()

# Calculating average times
for func in times:
    if len(times[func]) == 0:
        continue
    avg_time = sum(times[func]) / len(times[func])
    avg_time = round(avg_time, 3)
    print(f'Avg of {func}: {avg_time} seconds')


