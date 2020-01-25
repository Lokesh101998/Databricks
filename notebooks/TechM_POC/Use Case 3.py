# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.techmhpocadls.dfs.core.windows.net","rhmK/rr+BTE5K5xXITrdIKIL8QiAOjJMvFwafujf93DST4jr6JFgQHY5HC1K/DlYi6W5MfVyG9QTSTJ9Ae5d/Q==")

# COMMAND ----------

# DBTITLE 1,Reading Master Data:-MachineHistory
machine_history_df=spark.read.option("header",True).csv("abfss://techmpocdata@techmhpocadls.dfs.core.windows.net/tbl_machineHistory/")

# COMMAND ----------

#to change data type of Createdon and ID column
createdon_id_transform_df=(machine_history_df
                         .withColumn('CreatedOn',from_unixtime(unix_timestamp('CreatedOn', 'MM/dd/yyy')).cast(DateType()))
                         .withColumn('ID',col('ID').cast('int'))
                          )

# COMMAND ----------

# DBTITLE 1,Use Case: 3 CPU Utilization In Percentage
#changing data format of Cpuusage column
cpu_usage_transform_df=(
  createdon_id_transform_df.withColumn('CPUUsage', regexp_replace('CPUUsage' , '%', ''))
  .withColumn('ID',col('ID').cast("int"))
)
#to find the cpu_utilization by each ID per day
cpu_usage_df=(cpu_usage_transform_df.withColumn('year',year('CreatedOn')).withColumn('month',month('CreatedOn'))
                            .groupBy('ID','CreatedOn','year','month')
                            .agg(sum('CPUUsage').alias('cpu_utilization'))
                            .orderBy(col('ID').asc(),col('CreatedOn').asc())
              )
display(cpu_usage_df)

# COMMAND ----------

#formaing Table
cpu_usage_df.write.mode("overwrite").saveAsTable("cpu_usage_table_1") 

# COMMAND ----------

trend_of_cpu_utilization_df=(cpu_usage_df
                             .withColumn('month',month(col('CreatedOn')))
                             .withColumn('year',year(col('CreatedOn')))
                             .groupBy('ID','year','month').agg(sum('cpu_utilization')
                             .alias('cpu_utilization_in_percentage'))
                             .orderBy(col('ID').asc())
                            )
display(trend_of_cpu_utilization_df)

# COMMAND ----------

trend_of_cpu_utilization_df.write.saveAsTable("month_wise_trend_of_cpu_utilization")

# COMMAND ----------

# DBTITLE 1,Ram Utilization in MBs
#changing format of data of Ramusage column
ram_usage_transform_df=(createdon_id_transform_df
                        .withColumn('RAMUsage', regexp_replace('RAMUsage' , 'MB', ''))
                        .withColumn('RAMSpace', regexp_replace('RAMSpace' , 'MB', '')))
     
  #to find ram utilization by each ID per day
ram_usage_df=(ram_usage_transform_df
                           .withColumn('year',year(col('createdOn')))
                           .withColumn('month',month(col('CreatedOn')))
                           .groupBy('ID','CreatedOn','year','month')
                           .agg(sum('RAMSpace').alias('ram_space'),sum('RAMUsage').alias('ram_utilization'))
                           .withColumn("ram_free_space",col("ram_space") - col("ram_utilization"))
                           .orderBy(col('ID').asc(),col('CreatedOn').asc())
             )
display(ram_usage_df)

# COMMAND ----------

#forming table 
ram_usage_df.write.saveAsTable('RAM_Usages')

# COMMAND ----------

year_wise_ram_utilization_df=(ram_usage_df
                              .select('ID','CreatedOn',year(col('CreatedOn'))
                              .alias('year'),'ram_utilization')
                             )
display(year_wise_ram_utilization_df)

# COMMAND ----------

# DBTITLE 1, Hard Disk Utilization in GBs
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import re
def fun_cast(str):
    digit = re.findall('\d+', str) 
    digit_list = list(map(int, digit))
    total_space=0
    for i in digit_list:
        total_space+=i
    return(total_space)
spark_udf = udf(fun_cast, IntegerType())

hard_disk_space_transform=(createdon_id_transform_df
                           .withColumn('disk_space_GB',spark_udf('HardDiskSpace'))
                           .withColumn('disk_free_space_GB',spark_udf('HardDiskFreeSpace'))
                          )

hard_disk_usage_df=(hard_disk_space_transform
                           .groupBy('ID','CreatedOn')
                           .agg(sum('disk_space_GB').alias('disk_space'),sum('disk_free_space_GB').alias('disk_space_free'))
                           .withColumn("disk_space_used",col("disk_space") - col("disk_space_free"))
                           .orderBy(col('ID').asc(),col('CreatedOn').asc())
                   )


# COMMAND ----------

#forming Table
hard_disk_usage_df.write.saveAsTable("hard_disk_usage")

# COMMAND ----------

top_10_hard_disk_space_used_machine=(hard_disk_usage_df
                                      .select('disk_space_used','ID','CreatedOn')
                                      .orderBy(col('disk_space_used').desc())
                                      .limit(10)
                                     )                                     
display(top_10_hard_disk_space_used_machine)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table top_10_hard_disk_space_used_machine

# COMMAND ----------

month_wise_hard_disk_consumption=(hard_disk_usage_df
                                  .withColumn('month',month(col('CreatedOn')))
                                  .withColumn('year',year(col('CreatedOn')))
                                  .groupBy('ID','year','month').agg(sum('disk_space_used')
                                                                    .alias('disk_space_used_GB'))
                                  .orderBy(col('ID').asc())
                                 )
display(month_wise_hard_disk_consumption)

# COMMAND ----------

# DBTITLE 1,User  Being Online in Minutes
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import re
def minutes(st):
    digit=re.findall('\d+' ,st)
    total=list(map(int,digit))
    for i in total:
        day=total[0]*24*60
        hour=total[1]*60
        minute=total[2]
        return (day+hour+minute)
minutes_udf = udf(minutes, IntegerType())

system_uptime_transform=(createdon_id_transform_df
                         .withColumn("minute",minutes_udf('SystemUptime'))
                        )

system_uptime_df=(system_uptime_transform.withColumn('year',year('CreatedOn'))
                  .groupBy("ID","CreatedOn","year","UserName")
                  .agg(sum('minute').alias("system_uptime_minutes"))
                  .orderBy(col('ID').asc(),col('CreatedOn').asc())
                 )
display(system_uptime_df)

# COMMAND ----------

#forming Table
system_uptime_df.write.saveAsTable("User_Being_Online")

# COMMAND ----------

#Top 10 System Uptime Machine
top_10_system_uptime_machine=(system_uptime_df
                              .groupBy('ID'
                                       ,'UserName').agg(max('system_uptime_minutes').alias('system_uptime_minutes'))
                              .orderBy(col('system_uptime_minutes').desc())
                              .limit(10)
                             )
display(top_10_system_uptime_machine)

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table top_10_user_online

# COMMAND ----------

top_10_system_uptime_machine.write.saveAsTable("top_10_user_online")

# COMMAND ----------

# DBTITLE 1,System Uptime 
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType
def dayParsing(time):
    days = time // (24 * 60)
    time = time % (24 * 60)
    hours = time // 60
    time = time %60
    minutes = time 
    return(str(days) + ' days ' + str(hours) + ' hours '+ str(minutes) + ' minutes') 
dayparsing_udf=udf(dayParsing,StringType())

system_uptime_day=(system_uptime_df
                   .withColumn('updays',dayparsing_udf('system_uptime_minutes'))
                  )

display(system_uptime_day)

# COMMAND ----------

machine_history_is_active=(createdon_id_transform_df
                           .groupBy('ID','CreatedOn')
                           .agg(sum('IsActive').alias('is_active'))
                           .orderBy(col('ID').asc(),col('CreatedOn').asc())
                           )
display(machine_history_is_active)