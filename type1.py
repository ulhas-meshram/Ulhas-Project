#TYPE1#
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("TYPE1").getOrCreate()

    df1 = spark.read.csv("D:\input_data/table1.csv", header = True)

    df2 = spark.read.csv("D:\input_data/table2.csv", header = True)

    unionDF = df1.union(df2)

    withdatee = unionDF.select(col("*"), to_date(col("DOJ"), "dd-MM-yyyy").alias("Date_Of_Join"))
    withdatee.show()
    withdatee.printSchema()

    windowSpec = Window.partitionBy("id").orderBy(col("Date_Of_Join").desc())

    denseRank = withdatee.withColumn("Dense_Rank", dense_rank().over(windowSpec))
    denseRank.show()

    rankOne = denseRank.filter(denseRank.Dense_Rank == 1)

    finalRecord = rankOne.drop("Dense_Rank","DOJ")
    finalRecord.show()