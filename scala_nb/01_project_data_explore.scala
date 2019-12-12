// Databricks notebook source
val data = spark.read.option("inferSchema", "True").option("header", "True").csv("dbfs:/autumn_2019/dcashon/project/2018_jan.csv")

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

import scala.collection.mutable.ArrayBuffer
val basePath = "dbfs:/autumn_2019/dcashon/project/195646141_T_ONTIME_MARKETING_2018_"
val s = ArrayBuffer[String]()
for (i <- 2 to 12) {
  s += basePath + i + ".csv"
}

// COMMAND ----------

val all_data = s.map(spark.read.option("inferSchema", "True").option("header", "True").csv)

// COMMAND ----------

for (df <- all_data)
{
  df.printSchema()
}

// COMMAND ----------

val combined_data = all_data.reduce(_ unionByName _)

// COMMAND ----------

val true_combined = combined_data.unionByName(data)

// COMMAND ----------

true_combined.count()

// COMMAND ----------

all_data.map(_ count)

// COMMAND ----------

tru_com

// COMMAND ----------

true_combined.printSchema()

// COMMAND ----------

display(true_combined.filter($"MONTH" < 7).groupBy("OP_UNIQUE_CARRIER").count().orderBy("count"))

// COMMAND ----------

true_combined.write.parquet("dbfs:/autumn_2019/dcashon/project/2018.parquet")

// COMMAND ----------

val testread = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2018.parquet")

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

display(testread.select("YEAR").distinct().orderBy("MONTH"))

// COMMAND ----------

// get the 2019 data
val bpath1 = "dbfs:/autumn_2019/dcashon/project/195646141_T_ONTIME_MARKETING_2019_"
val bpath2 = "dbfs:/autumn_2019/dcashon/project/195828772_T_ONTIME_MARKETING_2019_"
val s2 = ArrayBuffer[String] ()
for (i <- 1 to 9) {
  if (i < 5) {
      s2 += bpath + i + ".csv"
  } else {
    s2 += bpath2 + i + ".csv"
  }

}

// COMMAND ----------

val data2019 = s2.map(spark.read.option("inferSchema", "True").option("header", "True").csv).reduce(_ unionByName _ )

// COMMAND ----------

data2019.write.parquet("dbfs:/autumn_2019/dcashon/project/2019.parquet")

// COMMAND ----------

val test = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2019.parquet")

// COMMAND ----------

display(test.filter($"MONTH" < 7).groupBy("OP_UNIQUE_CARRIER").count())

// COMMAND ----------


