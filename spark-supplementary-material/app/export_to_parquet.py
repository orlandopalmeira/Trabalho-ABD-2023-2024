from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, concat, lit

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


#> write the data to basic parquet
"""
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
"""

"""
#> Adicionar uma coluna para depois fazer partição por essa coluna mais abrangente (PIOROU DE 11 PARA 16 SECS)
answers = answers.withColumn('creationyear', year(answers.CreationDate))
answers.write.parquet(f'{path_to_data}answers_parquet_part_year', partitionBy='creationyear')
questions = questions.withColumn('creationyear', year(questions.CreationDate))
questions.write.parquet(f'{path_to_data}questions_parquet_part_year', partitionBy='creationyear')
comments = comments.withColumn('creationyear', year(comments.CreationDate))
comments.write.parquet(f'{path_to_data}comments_parquet_part_year', partitionBy='creationyear')
"""



#> Adicionar uma coluna para depois fazer partição por essa coluna mais abrangente (YEAR_MONTH) (AINDA ASSIM CRIA MUITAS PARTIÇÕES, E PIORA EXEC_TIME)
"""
answers = answers.withColumn('creationyearmonth', 
                             concat(year(answers.CreationDate), 
                                    lit('-'), 
                                    month(answers.CreationDate)))
answers.write.parquet(f'{path_to_data}answers_parquet_part_yearmonth', partitionBy='creationyearmonth')
"""


#> Orderby e depois write to parquet(NAO SE NOTOU MELHORIA, TENDO FICADO IGUAL E AS VEZES PIOR)
"""
answers_sorted = answers.orderBy('creationdate')
questions_sorted = questions.orderBy('creationdate')
comments_sorted = comments.orderBy('creationdate')
answers_sorted.write.parquet(f'{path_to_data}answers_sorted_parquet')
questions_sorted.write.parquet(f'{path_to_data}questions_sorted_parquet')
comments_sorted.write.parquet(f'{path_to_data}comments_sorted_parquet')
"""

#> Partitioning parquet files (isto cria ficheiros para cada valor especifico de creationdate, o que NÃO SERVE)
"""
# answers.write.parquet(f'{path_to_data}answers_parquet_partitioned_creationdate', partitionBy='creationdate')
# questions.write.parquet(f'{path_to_data}questions_parquet_partitioned_creationdate', partitionBy='creationdate')
# comments.write.parquet(f'{path_to_data}comments_parquet_partitioned_creationdate', partitionBy='creationdate')
"""    



# Ficha 5 exemplos

"""
# Using the DataFrame function write.parquet(out folder), export the titles DataFrame to Parquet
titles.write.parquet('/app/titles_parquet') # Dá erro se tentar escrever isto e já existir a pasta com o mesmo nome

# Export the DataFrame again, using gzip compression, by providing the compression=’gzip’ parameter to write.parquet.
titles.write.parquet('/app/titles_parquet_gzip', compression='gzip')

# Export the DataFrame again, partitioned by the startYear column (partitionBy=’startYear’)
titles.write.parquet('/app/titles_parquet_partitioned', partitionBy='startYear')
"""
