from main import timeit

from pyspark.sql.functions import count, avg, round as spark_round, desc, asc, col
from pyspark.sql.types import IntegerType
from pyspark.sql.dataframe import DataFrame


@timeit
def q3(tags: DataFrame, questionstags: DataFrame, answers: DataFrame, inferiorLimit: IntegerType = 10):
    # Subquery 1
    subquery1 = questionstags.groupBy("tagid").agg(count("*").alias("total")) \
        .filter(col("total") > inferiorLimit) \
        .select("tagid")

    # Subquery 2
    subquery2 = tags.join(questionstags, tags["id"] == questionstags["tagid"]) \
        .join(answers, answers["parentid"] == questionstags["questionid"], "left") \
        .groupBy("tagname", "questionid") \
        .agg(count("*").alias("total")) \
        .join(subquery1, subquery1["tagid"] == questionstags["tagid"], "inner") \
        .select("tagname", "total")

    # Main query
    result = subquery2.groupBy("tagname") \
        .agg(spark_round(avg("total"), 3).alias("avg_total"), count("*").alias("count")) \
        .orderBy(col("avg_total").desc(), col("count").desc(), "tagname")

    # Show the result
    result.show()

    # return result.collect()

@timeit
def q3_mv(mat_view: DataFrame, inferiorLimit: IntegerType = 10):
    result = mat_view.filter(col("count") > inferiorLimit)
    # result.show()
    return result.collect()
