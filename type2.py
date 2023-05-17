# #TYPE2#
# from pyspark.sql import SparkSession
# from pyspark.sql import Window
# from pyspark.sql.functions import *
# if __name__ == "__main__":
#     spark = SparkSession.builder.master("local[*]").appName("TYPE1").getOrCreate()
#
#     df1 = spark.read.csv("D:\input_data/table1.csv", header = True)
#     df2 = spark.read.csv("D:\input_data/table2.csv", header = True)
#
#     unionDF = df1.union(df2)
#
#     unionDF.printSchema()
#     unionDF.show()
#
#     withdatee = unionDF.select(col("*"),to_date(col("DOJ"),"dd-MM-yyyy").alias("Date_Of_Join"))
#     withdatee.show()
#     withdatee.printSchema()
#
#     windowSpec = Window.partitionBy("id").orderBy(col("Date_Of_Join"))
#
#     denseRank = withdatee.withColumn("Dense_Rank", dense_rank().over(windowSpec))
#     denseRank.show()
#
#     leadDF = denseRank.withColumn("Lead_Column",lead("DOJ",1).over(windowSpec))
#     leadDF.show()
#     leadDF.printSchema()
#
#     finalRecord = leadDF.drop("Dense_Rank")
#     finalRecord.show()
#
#------------------------------------------------------------

    # READ DATA FROM DATA SOURCE

    # IMPORT WHICH LIBRARY WE USE
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

    # CREATE SPARK SESSION
spark = SparkSession.builder.appName("Write to Oracle").getOrCreate()
url = "jdbc:oracle:thin:@localhost:1521/xe"
properties = {
        "user": "system",
        "password": "12131",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

dff = spark.read.jdbc(url=url, table="customers", properties=properties)