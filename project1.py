from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("ABCD").getOrCreate()
print(spark)



# df1 = spark.read.csv(r"D:\input_data\table1.csv", header = True)
# df1.show()
#
# df2 =df1.filter(df1.Name =='Ram')
# df2.show()
#
# df3= df1.filter(col("name")=='Amit')
# df3.show()
#
# df4 = df1.withColumn("date",split("DOJ","-").getItem(0))
# df4 = df4.withColumn("month",split("DOJ","-").getItem(1))
# df4 = df4.withColumn("year",split("DOJ","-").getItem(2))
# df4 = df4.drop("DOJ")
# df4.show()


# TO READ THE CSV FILE WITH COMPLETE DATA IN FILE Data_CSV.csv
df_Data_CSV = spark.read.csv(r"D:\input_data\Data_CSV_FILES\Data_CSV.csv",header = True)
# df_Data_CSV.show()
# counts = df_Data_CSV.count()
# print("Number of rows in the DataFrame df_Data_CSV is: ", counts)

# fINDING THE DISTINCT VALUES OF TABLE_NUMBER FROM 'df11'
distinct_values = df_Data_CSV.select('Table_number').distinct()
distinct_values.orderBy('Table_number').show(500)

# FILTERING THE DATA FROM "df11", HAVING DATA WITH TABLE_NUMBER STARTS WITH '73'
df73 = df_Data_CSV.filter(df_Data_CSV.Table_number.like ('73%'))
count73 = df73.count()
print("count of df with 73 is",count73)

# df73.write.csv(r"D:\input_data\Data_CSV_FILES\1\ghi.csv")
# df73.coalesce(1).write.mode("overwrite").option('header','True').csv(r"D:\input_data\Data_CSV_FILES\resultant table733.csv")
# df73.write.format("csv").option("header", "true").save("D:\input_data\Data_CSV_FILES\resultant table731.csv")

# dataframe df73 is not saved with specific filename, to overcome this issue we will use Pandas
# pandasdf73 = df73.toPandas()
# print(pandasdf73)


# filename = 'resultant table73.csv'
# folderpath = 'D:\input_data\Data_CSV_FILES\ '

# pandasdf73.to_csv(r"D:\input_data\Data_CSV_FILES\resultant table73.csv", index = False)

# FILTERING THE DATA FROM "df11", HAVING DATA WITH TABLE_NUMBER STARTS WITH '75'
df75 = df_Data_CSV.filter(df_Data_CSV.Table_number.like ('75%'))
count75 = df75.count()
print("count of df with 75 is",count75)
df75.show()

pandasdf75= df75.toPandas()
print(pandasdf75)

pandasdf75.to_csv(r"D:\input_data\Data_CSV_FILES\resultant table75.csv", index = False)

# print(f"total of {count73} + {count75} is", count73+count75)



