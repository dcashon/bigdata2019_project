// Databricks notebook source
import org.apache.spark.sql.functions._ // for later
val data18 = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2018.parquet")
val data19 = spark.read.parquet("dbfs:/autumn_2019/dcashon/project/2019.parquet")
val all = data18.unionByName(data19)

// COMMAND ----------

// get the metadata of the data..
/*
  1) number of unique carriers
  2) total number of flights
  3) number of unique airports
  4) start mo = Jan 2018, end mo = Sept 2019
*/
val num_total = all.count()
val unique_carriers = all.select("OP_UNIQUE_CARRIER").distinct() // will map with python
val num_unique_carriers = unique_carriers.count()
val num_unique_airports = all.select("DEST_AIRPORT_ID").distinct().count()

// write if needed
unique_carriers.write.csv("unique_carriers.csv")

// COMMAND ----------

all.printSchema()

// COMMAND ----------

// remember the field names
all.columns

// COMMAND ----------

// carrier distribution, all data
display(all.groupBy("OP_UNIQUE_CARRIER").count())

// COMMAND ----------

// wanted to start by getting general trends
// trends with time
// unable to compare fairly by year since 2019 is not quite over, have only 9 mo of data

// month on month growth from previous year, by number of flights
val mo18 = all.filter($"YEAR" === 2018).groupBy("MONTH").count()
val mo19 = all.filter($"YEAR" === 2019).groupBy("MONTH").count()
// join duplicates column names
val mtm = mo18.join(mo19.withColumnRenamed("count", "2019_count"), mo19("MONTH") === mo18("MONTH")).withColumn("abs_growth", $"2019_count" - $"count").withColumn("percent_growth", ($"2019_count" - $"count") / $"count" * 100).drop(mo19("MONTH")).orderBy("MONTH")
display(mtm)

// COMMAND ----------

// where are people going?
// just by state
val bystate = all.groupBy("DEST_STATE_NM").count().orderBy(desc("count")).limit(12)
display(bystate)

// COMMAND ----------

// lets zoom in on our state
val wastate = all.filter($"DEST_STATE_NM" === "Washington" || $"ORIGIN_STATE_NM" === "Washington")

// COMMAND ----------

// our most popular carriers (see other code for mapping of carrier codes via lookup table)
val carriers = wastate.groupBy("OP_UNIQUE_CARRIER").count().orderBy(desc("count")).limit(8)
display(carriers)

// COMMAND ----------

wastate.count().toFloat / all.count()

// COMMAND ----------

// first, compute and visualize some global statistics about the data
val bymo1 = data18.filter($"OP_UNIQUE_CARRIER" === "WN").groupBy("TAIL_NUM").count().orderBy("count")
val bymo2 = data19.filter($"OP_UNIQUE_CARRIER" === "WN").groupBy("TAIL_NUM").count().orderBy("count")}b

// COMMAND ----------

val test = data18.groupBy("OP_UNIQUE_CARRIER", "TAIL_NUM").count()

// COMMAND ----------

val test2 = test.groupBy("OP_UNIQUE_CARRIER").count().orderBy("count")

// COMMAND ----------

display(test2)

// COMMAND ----------

data18.select("TAIL_NUM").filter($"OP_UNIQUE_CARRIER" === "WN").take(20)

// COMMAND ----------

display(all.select("FLIGHTS").take(5))
