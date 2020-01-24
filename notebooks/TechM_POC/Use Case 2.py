# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Reading Master Data:- PatchDetails,MachineHistory,SoftwareListDetails
patch_details_df=spark.read.option("header",True).
csv("abfss://techmpocdata@techmhpocadls.dfs.core.windows.net/tbl_patchDetails/")
machine_history_df=spark.read.option("header",True).
csv("abfss://techmpocdata@techmhpocadls.dfs.core.windows.net/tbl_machineHistory/")
software_list_details_df=spark.read.option("header",True).
csv("abfss://techmpocdata@techmhpocadls.dfs.core.windows.net/tbl_softwareListDetails/dbo.TBL_SoftwareListDetails.csv/")

# COMMAND ----------

#to change data type of Createdon and ID column
machine_history_df=(machine_history_df
                         .withColumn('CreatedOn',from_unixtime(unix_timestamp('CreatedOn', 'MM/dd/yyy')).cast(DateType()))
                         .withColumn('ID',col('ID').cast('int'))
                          )
display(machine_history_df)

# COMMAND ----------

#Join the all dataframe on the basis of HostName of Patch dataframe
machine_history_join_patch_userwindow_software_df =(patch_details_df.alias('a')
                                          .join(machine_history_df.alias('b'), col('a.HostName') == col('b.HostName'),how='left')
                                         .join(software_list_details_df.alias('c'),col('a.HostName')==col('c.HostName'),how='left')   .select('a.HostName','b.CreatedOn','b.ID','b.IPLocation','c.Caption','c.Version')
)
display(machine_history_join_patch_userwindow_software_df)

# COMMAND ----------

# DBTITLE 1,Use Case:2 Analyse the incremental software changes
#to analyse s/w changes
software_changes_df=(machine_history_join_patch_userwindow_software_df
                    .select('HostName','CreatedOn','Caption','Version').distinct()
                    .orderBy(col('Version').asc(),col('CreatedOn').asc())
                    )
display(software_changes_df)

# COMMAND ----------

display(software_changes_df.select('caption').distinct())

# COMMAND ----------

caption_wise_software_changes=software_changes_df.groupBy('Hostname','Caption').agg(countDistinct('Version').alias('software_changes'))
display(caption_wise_software_changes)
#display(caption_wise_software_changes.filter(col('software_changes')>1).select('hostname'))

# COMMAND ----------

software_changes_df.write.saveAsTable('software_changes')

# COMMAND ----------

caption_wise_software_changes.write.saveAsTable('caption_wise_software_changes')

# COMMAND ----------

Host_wise_updated_version=software_changes_df.groupBy('HostName').agg(countDistinct('Version').alias('version'))
display(Host_wise_updated_version)

# COMMAND ----------

Host_wise_updated_version.write.saveAsTable('Host_wise_software_changes')