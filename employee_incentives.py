# from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("emp_incent").getOrCreate()

    employeeDF = spark.read.csv(r"D:\input_data/employee.csv", header=True)
    employeeDF.show()

    incentivesDF = spark.read.csv(r"D:\input_data/incentives.csv", header=True)
    incentivesDF.show()

    # incentivesDF.filter(incentivesDF["Employee_ref_id"] ==2).show()
    # QUE 3: Get all employee details from the employee table
    # employeeDF.select("*").show()

    # QUE 4: Get First_Name,Last_Name from employee table
    # employeeDF.select("First_name","Last_name").show()

    # QUE 5: Get First_Name from employee table using alias name “Employee Name”
    # employeeDF.select(col("first_name").alias("Employee Name")).show()

    # QUE 6: Get First_Name from employee table in upper case
    # employeeDF.select(upper("first_name")).show()

    # QUE 7: Select first 3 characters of FIRST_NAME from EMPLOYEE
    # TRY : substring("first_name",1,3)
    # employeeDF.select("first_name",substring("first_name",0,3).alias("substring")).show()

    # QUE 8: Get position of 'o' in name 'John' from employee table
    # POS=1 half solved
    # employeeDF.withColumn("substr_position", locate("o", col("first_name"),1)).show()

    # QUE 9: Get FIRST_NAME from employee table after removing white spaces from right side.
    # employeeDF.select("first_name",rtrim("first_name")).show()

    # QUE 10: Get First_Name from employee table after replacing 'o' with '$'
    # employeeDF.withColumn('first_name',translate('first_name','o','$')).show()

    # QUE 11: Get First_Name and Last_Name as single column from employee table separated by a '_'
    # employeeDF.select(concat_ws("_",employeeDF.First_name,employeeDF.Last_name).alias("FullName")).show()

    # QUE 12: Get FIRST_NAME, Joiningyear, Joining Month and Joining Date from employee table
    # employeeDF.select(col("first_name"),col("joining_date")).show()

    # employeeDF1= employeeDF.select(col("*"),to_date(col("joining_date"),"dd MMM yy").alias("Date_Of_Join"))
    # employeeDF1.show()
    # employeeDF1.printSchema()
    # ------------------------------------------------------------------------
    # employeeDF2 = employeeDF.select(col("*"), to_date(col("joining_date"),"dd-MMM-yy").alias("Date_Of_Join"))
    #
    # employeeDF2.show()
    #
    # # ------------------------------------------------------------------------
    # employeeDF3 = employeeDF2.select(col("*"), date_format(col("Date_Of_Join"), "dd-MMM-yy hh:MM:SS").alias("Date_Of_Join1"))
    # employeeDF3.printSchema()
    # employeeDF3.show()
    # #------------------------------------------------------------------------
    # employeeDF3.withColumn('dayofweek', dayofweek(col('Date_Of_Join'))).show()
    # # ------------------------------------------------------------------------
    # employeeDF3.withColumn('dayofmonth', dayofmonth(col('Date_Of_Join'))).show()
    # # ------------------------------------------------------------------------
    # employeeDF3.withColumn('dayofyear', dayofyear(col('Date_Of_Join'))).show()

    # QUE 13: Get all employee details from the employee table order by First_Name Ascending
    # employeeDF.orderBy(col("first_name").asc()).show()
    # employeeDF.orderBy(["first_name"],ascending = [True]).show()
    # employeeDF.sort(["first_name","salary"],ascending = [True]).show()
    # employeeDF.orderBy(col("first_name").asc(),col("Salary").desc()).show()

    # QUE 14: Get all employee details from the employee table order by First_Name Ascending and Salary Descending?
    # employeeDF.orderBy(col("first_name").asc(), col("Salary").desc()).show()

    # QUE 15: Get employee details from employee table whose employee name is “John”
    # employeeDF.filter(employeeDF["first_name"]== "John").show()
    # employeeDF.filter("first_name in ('John')").show()
    # employeeDF.filter(employeeDF.First_name == 'John').show()
    # employeeDF.select("*").where("First_name == 'John'").show()

    # QUE 16: Get employee details from employee table whose employee name are “John” and “Roy”
    # employeeDF.filter("first_name in ('John','Roy')").show()

    # QUE 17: Get employee details from employee table whose employee name are not “John” and “Roy”.
    # employeeDF.filter("first_name not in ('John','Roy')").show()

    # QUE 18: Get employee details from employee table whose first name starts with 'J'.
    # employeeDF.select("*").where("first_name like 'J%'").show()
    # employeeDF.filter(employeeDF.First_name.like ('J%')).show()

    # QUE 19: Get employee details from employee table whose first name contains 'o'.
    # employeeDF.where(col("first_name").contains("o")).show()

    # QUE 20: Get employee details from employee table whose first name ends with 'n'.
    # employeeDF.select("*").where("first_name like '%n'").show()
    # employeeDF.filter(employeeDF.First_name.like ('%n')).show()
    # employeeDF.filter(col("first_name").like('%n')).show()
    # employeeDF.filter("first_name like '%n'").show()

    # QUE 21: Get employee details from employee table whose first name ends with 'n' and name contains 4 letters.
    # employeeDF.select("*").where("first_name like '___n'").show()
    # employeeDF.filter("First_name like '___n'").show()
    # employeeDF.filter(employeeDF.First_name.like ('___n')).show()
    # employeeDF.filter(col("first_name").like('___n')).show()

    # QUE 22: Get employee details from employee table whose first name starts with 'J' and name contains 4 letters.
    # employeeDF.select("*").where("first_name like ('J___')").show()
    # employeeDF.filter("First_name like ('J___')").show()
    # employeeDF.filter(employeeDF.First_name.like ('J___')).show()
    # employeeDF.filter(col("first_name").like ('J___')).show()

    # QUE 23: Get employee details from employee table who’s Salary greater than 600000.
    # employeeDF.select("*").where("salary > 600000").show()
    # employeeDF.filter("salary > 600000").show()
    # employeeDF.filter(employeeDF.Salary > 600000).show()
    # employeeDF.filter(col("salary") > 600000).show()

    # QUE 24: Get employee details from employee table whose joining year is “2013”.
    # DF = employeeDF.select(col("*"),to_date(col("Joining_date"),"dd-MMM-yy").alias("DOJ"))
    # DF.printSchema()
    # DF.show()

    # -------------------------------------------------------
    # To Convert Column DOJ into given format
    # DF1 = DF.select(col("*"),date_format(col("DOJ"),"dd-MMM-yyyy").alias("date_in_format"))
    # DF1.show()
    # ----------------------------------------------------------

    # DF2 = DF.withColumn('years',year(DF.DOJ))
    # DF2.show()
    # ----------------------------------------------------------
    # DF2.select("*").where("years = 2013").show()
    # DF2.filter("years == 2013").show()
    # DF2.filter(col("years") == 2013).show()
    # DF2.filter(DF2.years == 2013).show()

    # ###---------OR---------#############
    # employeeDF.select("*").where("Joining_date  like('%13')").show()
    # employeeDF.filter("Joining_date like ('%13')").show()
    # employeeDF.filter(employeeDF.Joining_date.like ('%13')).show()
    # employeeDF.filter(col("Joining_date").like('%13')).show()

    # QUE 25: Get employee details from employee table whose joining month is “January”.
    # employeeDF.select("*").where("Joining_date like '%Jan%'").show()
    # employeeDF.filter("Joining_date like ('%Jan%')").show()
    # employeeDF.filter(employeeDF.Joining_date.like('%Jan%')).show()
    # employeeDF.filter(col("Joining_date").like('%Jan%')).show()

    # QUE 26: Get employee details from employee table who joined before January 1st 2013.
    # DF = employeeDF.select(col("*"),to_date(col("Joining_date"),"dd-MMM-yy").alias("Joining_datenew"))
    # DF.show()
    # DF.printSchema()

    # DF.select("*").where("Joining_datenew < '2013-01-01'").show()
    # DF.filter(DF.Joining_datenew <'2013-01-01').show()
    # DF.filter(col("Joining_datenew") <'2013-01-01' ).show()
    # DF.filter("Joining_datenew <'2013-01-01'").show()

    # QUE 27: Get employee details from employee table who joined after January 31st
    # DF.select("*").where("Joining_datenew > '2013-01-31'").show()
    # DF.filter("Joining_datenew > '2013-01-31'").show()
    # DF.filter(DF.Joining_datenew > '2013-01-31').show()
    # DF.filter(col("Joining_datenew") > '2013-01-31').show()

    # QUE 28: Get Joining Date and Time from employee table
    # DF3 = employeeDF.select(col("*"),to_date(col("Joining_date"),"dd-MMM-yy"))
    # DF3.show()

    # DF4 = employeeDF.withColumn("Joining_date",to_timestamp("Joining_date","dd-MMM-yy"))
    # # DF4.show()
    #
    # DF5 = DF4.withColumn("JoiningDate",split(DF4["Joining_date"]," ").getItem(0)).show()
    #
    # DF6 = DF4.withColumn("JoiningTime", split(DF4["Joining_date"], " ").getItem(1)).show()

    # QUE 29: Get Joining Date, Time including milliseconds from employee table
    # DF4 = employeeDF.withColumn("Joining_date", to_timestamp("Joining_date", "dd-MMM-yy"))
    #
    # DF4.show()
    #
    # DF4.withColumn("Joining_date",date_format("Joining_date","dd-MMM-yy HH:mm:ss.SS")).show(truncate = False)

    # QUE 30: Get difference between JOINING_DATE and INCENTIVE_DATE from employee and incentives table.
    # DF5 = employeeDF.withColumn("Joining_date",to_date("Joining_date","dd-MMM-yy"))
    # DF5.show()
    # DF5.printSchema()
    #
    # DF6 = incentivesDF.withColumn("Incentive_date",to_date("Incentive_date","dd-MMM-yy"))
    # DF6.show()
    # DF6.printSchema()
    #
    # DF7 = DF5.join(DF6,DF5.Emp_id == DF6.Employee_ref_id,"inner")
    #
    # DF7.withColumn("date_diff",datediff(col("Incentive_date"),col("Joining_date"))).show()

    # QUE 31: Get department, total salary with respect to a department from employee table.
    # DF8 = employeeDF.withColumn("Salary",employeeDF["Salary"].cast(IntegerType())).withColumn("Emp_id",employeeDF["Emp_id"].cast(IntegerType()))
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    # DF8.printSchema()
    # DF8.show()
    # DF8.groupBy("Department").sum().orderBy("Department").show()
    # DF8.groupBy("Department").sum("Salary").orderBy("Department").show()

    # QUE 32: Get department, total salary with respect to a department from employee table order by total salary descending.
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    #
    # DF8.groupBy("Department").sum("Salary").sort(desc('sum(Salary)')).show()
    # DF8.groupBy("Department").agg(sum("Salary").alias("salary_summ")).sort(desc('salary_summ')).show()

    # QUE 33: Get department, no of employees in a department, total salary with respect to a department from employee table order by total salary descending.
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    #
    # DF8.groupBy("Department").agg(count("Emp_id").alias("no_of_emp"),(sum("salary").alias("total_salary"))).sort(desc("total_salary")).show()

    # # QUE 34: Get department wise average salary from employee table order by salary ascending?
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    # DF8.groupBy("Department").agg(avg("Salary")).sort(asc("avg(Salary)")).show()
    # DF8.groupBy("Department").agg(avg("Salary").alias("average_salary")).sort(asc("average_salary")).show()

    # QUE 35: Get department wise maximum salary from employee table order by salary ascending?
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    # DF8.groupBy("Department").agg(max("salary").alias("max_salary")).sort(asc("max_salary")).show()

    # QUE 36: Get department wise minimum salary from employee table order by salary ascending?
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    # DF8.groupBy("department").agg(min("salary").alias("min_salary")).sort(asc("min_salary")).show()

    # QUE 37:Select no of employees joined with respect to year and month from employee table.

    # QUE 38: Select department,total salary with respect to a department from employee table where total salary greater than 800000 order by Total_Salary descending
    # DF8 = employeeDF.withColumn("Salary", employeeDF["Salary"].cast(IntegerType()))
    # DF9 = DF8.groupBy("Department").agg(sum("salary").alias("total_salary")).sort(desc("total_salary"))
    # DF9.show()
    # ----------------------------------
    # DF9.createOrReplaceTempView("table1")
    # DF10 = spark.sql("select * from table1 where total_salary > 800000 order by total_salary desc")
    # DF10.show()

    # QUE 39: Select employee details from employee table if data in incentive table?
    # employeeDF.join(incentivesDF,employeeDF.Emp_id == incentivesDF.Employee_ref_id,"inner").show()

    # QUE 40: Select 20 % of salary from John, 10% of Salary for Roy and for other 15 % of salary from employee table

    # QUE 41: Select Banking as 'Bank Dept', Insurance as 'Insurance Dept' and Services as    'Services Dept' from employee table.
    # employeeDF.withColumn("Department",regexp_replace('Department','Banking','Bank Dept')).withColumn("Department",regexp_replace('Department','Insurance','Insurance Dept')).withColumn("Department",regexp_replace('Department','Services','Services Dept')).show()

    # employeeDF.withColumn("Department",when(employeeDF.Department == 'Banking','Bank Dept').when(employeeDF.Department == 'Insurance','Insurance Dept').when(employeeDF.Department == 'Services','Services Dept')).show()

    # QUE 42: Delete employee data from employee table who got incentives in incentive table
    # employeeDF.join(incentivesDF, incentivesDF.Employee_ref_id == employeeDF.Emp_id,"leftanti").show()

    # QUE 43: Select first_name, incentive amount from employee and incentives table for those employees who have incentives.
    # employeeDF.join(incentivesDF,employeeDF.Emp_id == incentivesDF.Employee_ref_id,"inner").select(col("First_name"),col("Incentive_amount")).show()

    # QUE 44: Select first_name, incentive amount from employee and incentives table for those employees who have incentives and incentive amount greater than 3000
    # employeeDF.join(incentivesDF, employeeDF.Emp_id == incentivesDF.Employee_ref_id,"outer").select(col("First_name"),col("Incentive_amount")).where(col("Incentive_amount")>3000).show()

    # QUE 45: Select first_name, incentive amount from employee and incentives table for all employees even if they didn't get incentives
    # employeeDF.join(incentivesDF, employeeDF.Emp_id == incentivesDF.Employee_ref_id,"outer").select(col("First_name"), col("Incentive_amount")).show()

    # QUE 46: Select first_name, incentive amount from employee and incentives table for all employees even if they didn't get incentives and set incentive amount as 0 for those employees who didn't get incentives.
    # employeeDF.join(incentivesDF,employeeDF.Emp_id == incentivesDF.Employee_ref_id,"outer").select(col("First_name"),col("Incentive_amount")).na.fill("0").show()

    # QUE 47: Select first_name, incentive amount from employee and incentives table for all employees who got incentives using left join
    # incentivesDF.join(employeeDF, incentivesDF.Employee_ref_id == employeeDF.Emp_id,"left").select(col("First_name"),col("Incentive_amount")).show()

    # QUE 48: Select max incentive with respect to employee from employee and incentives table using sub query.
    # DF11 = incentivesDF.groupBy("Employee_ref_id").agg(max("Incentive_amount").alias("max_incentive"))
    # employeeDF.join(DF11,DF11.Employee_ref_id == employeeDF.Emp_id, "inner").show()
    # --------------------------------------------------
    # employeeDF.createOrReplaceTempView("employeeDF")
    # incentivesDF.createOrReplaceTempView("incentivesDF")
    #
    # spark.sql("select Salary from employeeDF").show()
    #
    # spark.sql("select Employee_ref_id,max(Incentive_amount) maxx from incentivesDF group by Employee_ref_id").show()

    # spark.sql("with table1(Employee_ref_id,maxx_incentive) as (select Employee_ref_id, max(Incentive_amount) maxx_incentive from incentivesDF group by Employee_ref_id) select employeeDF.First_name,table1.maxx_incentive from employeeDF left join table1 on table1.Employee_ref_id = employeeDF.Emp_id ").show()
