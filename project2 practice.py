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
# distinct_values = df_Data_CSV.select('Table_number').distinct()
# distinct_values.orderBy('Table_number').show(500)

# FILTERING THE DATA FROM "df11", HAVING DATA WITH TABLE_NUMBER STARTS WITH '73'
# df73 = df_Data_CSV.filter(df_Data_CSV.Table_number.like ('73%'))
# count73 = df73.count()
# print("count of df with 73 is",count73)
# df73.show()

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
# df75 = df_Data_CSV.filter(df_Data_CSV.Table_number.like ('75%'))
# count75 = df75.count()
# print("count of df with 75 is",count75)
# df75.show()

# pandasdf75= df75.toPandas()
# print(pandasdf75)
#
# pandasdf75.to_csv(r"D:\input_data\Data_CSV_FILES\resultant table75.csv", index = False)

# print(f"total of {count73} + {count75} is", count73+count75)

# df73_split = df73.withColumn("min",split("Amount","-").getItem(0))
# df73_split = df73_split.withColumn("max",split("Amount","-").getItem(1))
# df73_split = df73_split.withColumn("max",split("Amount","More than").getItem(0))
# df73_split.show()

# df731 = df73.filter(df73.Amount.like("%More than%") | df73.Amount.like("%or Less%") )
# df731.show()
# df732 = df731.select("Amount").distinct().orderBy("Amount")
# df733 = df731.select("Amount").distinct().orderBy(desc("Amount"))
# df732.show()
# df733.show()


# df73_replace = df73.withColumn("Amount",regexp_replace("Amount","More than 250,000","250,000 - 9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","More than 1,000,000","1,000,000 - 9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","More than 500,000","500,000 - 9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","More than 10,000,000","10,000,000 - 9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","More than 5,000,000","5,000,000 - 9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","1,000,000 or Less","0 - 1,000,000"))\
#     .withColumn("Amount",regexp_replace("Amount","100,000 or Less","0 - 100,000"))\
#     .withColumn("Symbol",regexp_replace("Symbol","More Than \\$250,000","\\$250,000 - \\$9999999999"))\
#     .withColumn("Amount",regexp_replace("Amount","50,000 or Less","0 - 50,000"))\
#     .withColumn("Amount",regexp_replace("Amount","500,000 or Less","0 - 500,000"))\
#     .withColumn("min",split("Amount","-").getItem(0))\
#     .withColumn("max",split("Amount","-").getItem(1)).drop("Amount")\
#     .withColumn("Symbol",regexp_replace(regexp_replace("Symbol","and",",")," ",""))\
#     .withColumn("Symbol",explode(split("Symbol",",")))\
#     .withColumn("deductible",regexp_replace("deductible",",",""))\
#     .select("State_code","Table_number","Effective_date","Exp_date","Terr_code","min","max","deductible","Symbol","Factor")

# df73_replace.show()
# print("count of df73_replace: ",df73_replace.count())
# df73_replace.orderBy(asc("State_code")).show()
#=====================================================================================

# FILTERING THE DATA FROM "df11", HAVING DATA WITH TABLE_NUMBER STARTS WITH '75'
df75 = df_Data_CSV.filter(df_Data_CSV.Table_number.like ('75%'))
# count75 = df75.count()
# print("count of df with 75 is",count75)
df75.show(5)

# to find distinct value in col  "Symbol"
# df751 = df75.filter(df75.Symbol.like ("%More Than%") | df75.Symbol.like ("%Or Less%"))
# df751.show()
# df751.select("Symbol").distinct().orderBy("Symbol").show(truncate = False)

df75_replace = df75.withColumn("Terr_code",regexp_replace(regexp_replace("Terr_code","and",",")," ",""))\
    .withColumn("Terr_code",explode(split("Terr_code",",")))\
    .withColumn("Amount",regexp_replace("Amount",",",""))\
    .withColumn("deductible",regexp_replace("deductible","%",""))\
    .withColumn("Symbol",regexp_replace("Symbol","\\$1,000,000 Or Less","0 - \\$1,000,000"))\
    .withColumn("Symbol",regexp_replace("Symbol","\\$100,000 Or Less","0 - \\$100,000"))\
    .withColumn("Symbol",regexp_replace("Symbol","\\$250,000 Or Less","0 - \\$250,000"))\
    .withColumn("Symbol",regexp_replace("Symbol","\\$50,000 Or Less","0 - \\$50,000"))\
    .withColumn("Symbol",regexp_replace("Symbol","\\$500,000 Or Less","0 - \\$500,000"))\
    .withColumn("Symbol",regexp_replace("Symbol","More Than \\$1,000,000","\\$1,000,000 - \\$9999999999"))\
    .withColumn("Symbol",regexp_replace("Symbol","More Than \\$10,000,000","\\$10,000,000 - \\$9999999999"))\
    .withColumn("Symbol",regexp_replace("Symbol","More Than \\$250,000","\\$250,000 - \\$9999999999"))\
    .withColumn("Symbol",regexp_replace("Symbol","More Than \\$5,000,000","\\$5,000,000 - \\$9999999999"))\
    .withColumn("Symbol",regexp_replace("Symbol","More Than \\$500,000","\\$500,000 - \\$9999999999"))\
    .withColumn("min",split("Symbol","-").getItem(0))\
    .withColumn("max",split("Symbol","-").getItem(1))\
    .select("State_code","Table_number","Effective_date","Exp_date","Terr_code","Amount","deductible","min","max","Factor")



print("this is df75_replace table ")
df75_replace.show(5000)

# cvf = df75_replace.filter(df75_replace.Symbol=="0 - $1,000,000")
# cvf.show()
print("count of df75_replace: ",df75_replace.count())

