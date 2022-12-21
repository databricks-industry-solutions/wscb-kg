// Databricks notebook source
// MAGIC %md
// MAGIC # Creating a Knowledge Graph from MeSH and Clinical Trials
// MAGIC 
// MAGIC ## Querying & Insights
// MAGIC 
// MAGIC This notebook will build an insight that will tell us the number trials related to brain neoplasms over time. This information would be impossible to get with either data set alone. MeSH has no trial information, and the conditions in clinical trials are just strings, there is no sense that a "choroid plexus papilloma" and a "neurocytoma" share a category. 

// COMMAND ----------

// DBTITLE 1,Imports
import com.graphster.orpheus.config.Configuration
import com.graphster.orpheus.config.graph.NodeConf
import com.graphster.orpheus.query.Query.implicits._
import org.apache.spark.sql.{functions => sf}
import org.apache.spark.sql.expressions.Window

import scala.language.postfixOps

import spark.implicits

// COMMAND ----------

// MAGIC %run "./01-config"

// COMMAND ----------

val triples = spark.table("mesh_nct.mesh_nct")
  .select(
    NodeConf.row2string($"subject").as("s"),
    NodeConf.row2string($"predicate").as("p"),
    NodeConf.row2string($"object").as("o"),
  ).persist()

// COMMAND ----------

display(triples)

// COMMAND ----------

val queryConfig = QueryConfig(config / "query" getConf)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we will pull all trials that study a condition that is a subtype of brain neoplasms, along with their title and date. First the code finds the entity that has name "Brain Neoplasms".
// MAGIC 
// MAGIC One thing to note here is that we are finding the brain neoplasms entity from the string "Brain Neoplasms". `REGEX` in SPARQL can be slow, but it allows us to get the entity without having to manually look up the ID. In most triple stores, this could result in a time-out, but since we are querying on Spark, we can easily scale out the processing.

// COMMAND ----------

val query = """
    |SELECT ?trial ?title ?date
    |WHERE {
    | ?brain_neoplasms rdfs:label ?label .
    | FILTER(REGEX(?label, "^Brain Neoplasms$", "i"))
    | ?brain_neoplasms rdf:type mv:TopicalDescriptor .
    | ?trial rdf:type schema:MedicalTrial .
    | ?trial schema:healthCondition ?c .
    | ?c mv:broaderDescriptor* ?brain_neoplasms .
    | ?trial schema:startDate ?date .
    | ?trial rdfs:label ?title .
    |}
    |""".stripMargin

val results = triples.sparql(queryConfig, query).convertResults(Map())

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's get the year for each trial.

// COMMAND ----------

val parsedResults = results.withColumn("year", sf.year($"date")).persist()

// COMMAND ----------

display(parsedResults)

// COMMAND ----------

// MAGIC %md
// MAGIC ---- TO BE COPIED 

// COMMAND ----------

// MAGIC %md
// MAGIC The dips in certain years are interesting, but the number of mentions is generally increasing over time. This is to be expected since the number of trials is expected to increase over time. Let's calculate a year-over-year increase.

// COMMAND ----------

display(parsedResults.groupBy("year").count().orderBy("year"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Here we see some spikes early on, but this may be to due to incomplete data for those years.

// COMMAND ----------

display(parsedResults.groupBy("year").count().withColumn("yoy_pct_increase", sf.lit(100) * sf.col("count") / sf.lag("count", 1, 1).over(Window.orderBy("year")) - sf.lit(100)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Conclusion
// MAGIC 
// MAGIC In these notebooks, we took two different data sets, fused them into a common knowledge graph, queried them, and got insights. From here there are several things we could do develop this more.
// MAGIC 
// MAGIC - Extract more information from clinical trials
// MAGIC - Parametrize the querying notebooks
// MAGIC - Add more data sets
// MAGIC 
// MAGIC The best thing about this is that we know have this data in a knowledge graph that is a more natural representation of facts than tables or indexes. This makes it easier for researchers to understand how to explore the data. This is just scratching the surface. We can look at extracting information from the text in the clinical trials data, we look at building link prediction models that look to expand the graph from latent information already contained. Of course, this is in addition to all the applications that could be powered directly from the graph.
