// Databricks notebook source
// D. Cashon
// test writing data to Azure SQL DB via Spark DataFrame

// below is copypasted from 
// https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-connect-to-sql-database
val jdbcUsername = "dcashon"
val jdbcPassword = "temppass420!"
val jdbcHostname = "bigdata2019.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase ="bigdata"

import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;rewriteBatchedStatements=true;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")


// COMMAND ----------

connectionProperties

// COMMAND ----------

// we should now be able to write to the AzureDB. Load the data:
val data18 = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2018.parquet")
val data19 = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2019.parquet")
val all = data18.unionByName(data19)

// COMMAND ----------

// here goes
all.createOrReplaceTempView("tempflights")
spark.sql("create table tempflight as select * from tempflights")

// COMMAND ----------


spark.table("tempflight").write.jdbc(jdbc_url, "flights", connectionProperties)

//spark.table("flighttable").write.format("jdbc").option("url", "jdbc:sqlserver://bigdata2019.database.windows.net:1433;database=bigdata;rewriteBatchedStatements=true;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;").option("batchsize", 1000).option("user", "dcashon").option("password", "temppass420!").option("dbtable", "flights").option("numPartitions", "10").save()

// COMMAND ----------

// according to here, this speeds things up
// https://docs.databricks.com/data/data-sources/sql-databases-azure.html
// but cannot install on cluster :C (permissions)
//import com.microsoft.azure.sqldb.spark.config.Config
//import com.microsoft.azure.sqldb.spark.connect._


// COMMAND ----------

// try again with larger batchsize for jdbc 

