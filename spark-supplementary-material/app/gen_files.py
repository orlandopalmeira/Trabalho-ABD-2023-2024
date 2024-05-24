import sys, inspect
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, year, expr

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



#******************************** QUERY 1 ********************************

Q1_PATH = f"{path_to_data}Q1/"

def q1_users():
    new_users = users.select("id", "displayname").repartition(3)
    new_users.write.mode("overwrite").parquet(f'{Q1_PATH}users_id_displayname')

def q1_ord():
    answers_ordered = answers.select('owneruserid', 'creationdate').orderBy('CreationDate')
    questions_ordered = questions.select('owneruserid', 'creationdate').orderBy('CreationDate')
    comments_ordered = comments.select(col('userid').alias('owneruserid'), 'creationdate').orderBy('CreationDate')

    answers_ordered.write.parquet(f'{Q1_PATH}answers_creationdate_ordered')
    questions_ordered.write.parquet(f'{Q1_PATH}questions_creationdate_ordered')
    comments_ordered.write.parquet(f'{Q1_PATH}comments_creationdate_ordered')


# Adicionar uma coluna (YEAR) para depois fazer partição por essa coluna mais abrangente e utilizando-a nas queries para permitir partition pruning
def q1_add_year_partition():
    new_answers = answers.withColumn('creationyear', year(answers.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
    new_answers.write.parquet(f'{Q1_PATH}answers_parquet_part_year', partitionBy='creationyear')
    new_questions = questions.withColumn('creationyear', year(questions.CreationDate)).select('OwnerUserId', 'CreationDate', 'creationyear')
    new_questions.write.parquet(f'{Q1_PATH}questions_parquet_part_year', partitionBy='creationyear')
    new_comments = comments.withColumn('creationyear', year(comments.CreationDate)).select('UserId', 'CreationDate', 'creationyear')
    new_comments.write.parquet(f'{Q1_PATH}comments_parquet_part_year', partitionBy='creationyear')


def q1_repartitionByRange():
    answers_rep = answers.select('owneruserid', 'creationdate').repartitionByRange(15, col('creationdate'))
    questions_rep = questions.select('owneruserid', 'creationdate').repartitionByRange(15, col('creationdate'))
    comments_rep = comments.select(col('userid').alias('owneruserid'), 'creationdate').repartitionByRange(15, col('creationdate'))
    answers_rep.write.mode("overwrite").parquet(f'{Q1_PATH}answers_creationdate_reprange')
    questions_rep.write.mode("overwrite").parquet(f'{Q1_PATH}questions_creationdate_reprange')
    comments_rep.write.mode("overwrite").parquet(f'{Q1_PATH}comments_creationdate_reprange')


def q1_gen_files():
    q1_users()
    q1_ord()
    q1_add_year_partition()
    q1_repartitionByRange()


#******************************** QUERY 2 ********************************

Q2_PATH = f"{path_to_data}Q2/"

def q2():
    year_range = spark.range(2008, int(spark.sql("SELECT year(CURRENT_DATE)").collect()[0][0] + 1), 1).toDF("year")
    max_reputation_per_year = users \
                                .withColumn("year", expr("year(creationdate)")) \
                                .groupBy("year") \
                                .agg({"reputation": "max"}) \
                                .withColumnRenamed("max(reputation)", "max_rep")

    users_selected = users.select('id','creationdate','reputation')

    answers_selected = answers.select('owneruserid', 'id')
    #answers_selected = answers_selected.withColumnRenamed('id', 'answer_id')

    votes_selected = votes.select('postid', 'creationdate', 'votetypeid')
    votes_selected = votes_selected.withColumnRenamed('creationdate', 'votes_creationdate')

    votesTypes_selected = votesTypes.filter(col("name") == "AcceptedByOriginator")
    votesTypes_selected = votesTypes_selected.select('id')
    #votesTypes_selected = votesTypes_selected.withColumnRenamed('id', 'votesTypes_id')

    accepted_answers = (
        votes_selected.join(votesTypes_selected, votes_selected["votetypeid"] == votesTypes_selected["id"])
            .select("postid", "votes_creationdate")
    )

    filtered_answers = (
        answers_selected.join(accepted_answers, answers["id"] == accepted_answers["postid"])
            .select("owneruserid","votes_creationdate")
    )

    u = (
        users_selected.join(filtered_answers, users_selected["id"] == filtered_answers["owneruserid"])
            .select(users_selected["id"], users_selected["creationdate"], users_selected["reputation"], filtered_answers["votes_creationdate"])
    )

    year_range.write.mode("overwrite").parquet(f'{Q2_PATH}year_range')
    max_reputation_per_year.write.mode("overwrite").parquet(f'{Q2_PATH}max_reputation_per_year')
    u.write.mode("overwrite").parquet(f'{Q2_PATH}u')

    # versão com ord

    u_ord = u.orderBy('votes_creationdate')
    u_ord.write.mode("overwrite").parquet(f'{Q2_PATH}u_ord')

    # versão com ord e part

    u_ord_part = u_ord.repartitionByRange(col('votes_creationdate'))
    u_ord_part.write.mode("overwrite").parquet(f'{Q2_PATH}u_ord_part')




#******************************** QUERY 3 ********************************

Q3_PATH = f"{path_to_data}Q3/"
def q3_create_files():
    tags.write.parquet(f'{Q3_PATH}tags_parquet')
    questionsTags.write.parquet(f'{Q3_PATH}questionsTags_parquet')
    answers.write.parquet(f'{Q3_PATH}answers_parquet')

def q3_create_mv():
    questionsTags.createOrReplaceTempView("questionstags")
    answers.createOrReplaceTempView("answers")
    tags.createOrReplaceTempView("tags")
    spark.sql("""
        SELECT qt.tagid, qt.questionid, COUNT(*) AS total
        FROM questionstags qt
        LEFT JOIN answers a ON a.parentid = qt.questionid
        GROUP BY qt.tagid, qt.questionid
    """).createOrReplaceTempView("TagQuestionCounts")
    spark.sql("""
        SELECT tagid, tagname, count(*) as tag_count
        FROM TagQuestionCounts tqc
        LEFT JOIN tags t ON t.id = tqc.tagid
        GROUP BY tagid, tagname
    """).createOrReplaceTempView("FilteredTags")
    result = spark.sql("""
        SELECT ft.tagname, ROUND(AVG(tqc.total), 3), ft.tag_count as count
        FROM TagQuestionCounts tqc
        JOIN FilteredTags ft ON ft.tagid = tqc.tagid
        GROUP BY ft.tagname, ft.tag_count
    """)

    result.write.mode("overwrite").parquet(f"{Q3_PATH}mv_parquet")

def q3_create_mv_ord():
    questionsTags.createOrReplaceTempView("questionstags")
    answers.createOrReplaceTempView("answers")
    tags.createOrReplaceTempView("tags")
    spark.sql("""
        SELECT qt.tagid, qt.questionid, COUNT(*) AS total
        FROM questionstags qt
        LEFT JOIN answers a ON a.parentid = qt.questionid
        GROUP BY qt.tagid, qt.questionid
    """).createOrReplaceTempView("TagQuestionCounts")
    spark.sql("""
        SELECT tagid, tagname, count(*) as tag_count
        FROM TagQuestionCounts tqc
        LEFT JOIN tags t ON t.id = tqc.tagid
        GROUP BY tagid, tagname
    """).createOrReplaceTempView("FilteredTags")
    result = spark.sql("""
        SELECT ft.tagname, ROUND(AVG(tqc.total), 3), ft.tag_count as count
        FROM TagQuestionCounts tqc
        JOIN FilteredTags ft ON ft.tagid = tqc.tagid
        GROUP BY ft.tagname, ft.tag_count
    """).sort("count").repartition(3)

    result.write.mode("overwrite").parquet(f"{Q3_PATH}mv_parquet_ord")

def q3():
    q3_create_files()
    q3_create_mv()
    q3_create_mv_ord()


#******************************** QUERY 4 ********************************
Q4_PATH = f"{path_to_data}Q4/"

def q4_create_badges_mv():
    badges.createOrReplaceTempView("badges")

    mat_view = spark.sql("""
    SELECT date
    FROM badges
    WHERE NOT tagbased
        AND name NOT IN (
            'Analytical',
            'Census',
            'Documentation Beta',
            'Documentation Pioneer',
            'Documentation User',
            'Reversal',
            'Tumbleweed'
        )
        AND class in (1, 2, 3)
        AND userid <> -1 """).repartition(9)
    
    mat_view.write.mode("overwrite").parquet(f"{Q4_PATH}badges_mat_view")


def q4_create_badges_mv_ord():
    badges.createOrReplaceTempView("badges")

    mat_view = spark.sql("""
    SELECT date
    FROM badges
    WHERE NOT tagbased
        AND name NOT IN (
            'Analytical',
            'Census',
            'Documentation Beta',
            'Documentation Pioneer',
            'Documentation User',
            'Reversal',
            'Tumbleweed'
        )
        AND class in (1, 2, 3)
        AND userid <> -1 """).repartitionByRange(9, "date")
    
    mat_view.write.mode("overwrite").parquet(f"{Q4_PATH}badges_mat_view_ord")



def q4():
    q4_create_badges_mv()
    q4_create_badges_mv_ord()




if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Running pre-defined function...")
        q4_create_badges_mv()

    elif sys.argv[1] == "all":
        print("Running all functions defined in the file...")
        functions = [o for o in inspect.getmembers(sys.modules[__name__]) if inspect.isfunction(o[1])]

        for function in functions:
            print(f"Running {function[0]}...")
            function[1]()

    else:
        print("Running custom function...")
        locals()[sys.argv[1]]()
