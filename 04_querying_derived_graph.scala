// Databricks notebook source
// MAGIC %md
// MAGIC # Creating a Knowledge Graph from MeSH and Clinical Trials

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

// MAGIC %run ./01_config

// COMMAND ----------

// MAGIC %md
// MAGIC ## Querying & Derived Graph
// MAGIC 
// MAGIC By integrating biomedical data in an connected manner helps in quick retrieval of hidden insights. These semantic networks also help reduce errors and increase chances of making a discovery in a cost-effective manner. For uncovering hidden correlations between medical data, analysts use different techniques like link prediction. By visually exploring these correlations between medical entities, scientists can make timely decisions on sensitive treatment options.
// MAGIC 
// MAGIC In this notebook we will query our graph on Spark, using the query module. This module is powered by the Bellman library from GSK.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC First, we will transform the graph into the format that Bellman requires for querying.

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
// MAGIC ### Brain Diseases
// MAGIC Now let's find analyses in the graph, corresponding to This query below finds all trials for brain diseases, MeSH ui: [D001927](https://meshb-prev.nlm.nih.gov/record/ui?ui=D001927)
// MAGIC and group by the type of brain disease and the intervention used.

// COMMAND ----------

val query = """
    |SELECT ?cond ?condLabel ?interv ?intervLabel (COUNT(?trial) AS ?numTrials)
    |WHERE {
    | ?cond mv:broaderDescriptor :D001927 .      # get all immediate children of brain disease
    | ?trial rdf:type schema:MedicalTrial .      # get all clinical trials
    | ?trial schema:healthCondition ?c .         # get the condition being studied
    | ?trial schema:studySubject ?interv .       # get the intervention being studied
    | ?c rdf:type mv:TopicalDescriptor .         # limit to conditions that were linked to MeSH, and to the descriptor matches
    | ?c mv:broaderDescriptor* ?cond .           # limit to conditions that sub types of some immediate child of brain disease
    | ?cond rdfs:label ?condLabel .              # get the label for the immediate child of brain disease
    | ?interv rdf:type mv:TopicalDescriptor .    # limit to interventions that were linked to MeSH, and to the descriptor matches
    | ?interv rdfs:label ?intervLabel .          # get the label for the intervention
    |}
    |GROUP BY ?cond ?condLabel ?interv ?intervLabel # group by the immediate child of brain disease and the intervention
    |""".stripMargin

val results = triples.sparql(queryConfig, query).convertResults(Map("numTrials" -> "int"))

// COMMAND ----------

results.createOrReplaceTempView("barainDiseasesResults")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE
// MAGIC     mesh_nct.brain_diseases
// MAGIC     AS (SELECT * from barainDiseasesResults)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from mesh_nct.brain_diseases

// COMMAND ----------

// MAGIC %md
// MAGIC #TODO : skip this analysis
// MAGIC ### Brain Neoplasms
// MAGIC Now we will pull all trials that study a condition that is a subtype of brain neoplasms, along with their title and date. First the code finds the entity that has name "Brain Neoplasms".
// MAGIC 
// MAGIC One thing to note here is that we are finding the brain neoplasms entity from the string "Brain Neoplasms". `REGEX` in SPARQL can be slow, but it allows us to get the entity without having to manually look up the ID. In most triple stores, this could result in a time-out, but since we are querying on Spark, we can easily scale out the processing.

// COMMAND ----------

// val query = """
//     |SELECT ?trial ?title ?date
//     |WHERE {
//     | ?brain_neoplasms rdfs:label ?label .
//     | FILTER(REGEX(?label, "^Brain Neoplasms$", "i"))
//     | ?brain_neoplasms rdf:type mv:TopicalDescriptor .
//     | ?trial rdf:type schema:MedicalTrial .
//     | ?trial schema:healthCondition ?c .
//     | ?c mv:broaderDescriptor* ?brain_neoplasms .
//     | ?trial schema:startDate ?date .
//     | ?trial rdfs:label ?title .
//     |}
//     |""".stripMargin

// val results = triples.sparql(queryConfig, query).convertResults(Map())

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's add the year for each trial.

// COMMAND ----------

// val parsedResults = results.withColumn("year", sf.year($"date")).persist()

// COMMAND ----------

// parsedResults.createOrReplaceTempView("barainNeoplasmsResults")

// COMMAND ----------

-- %sql
-- CREATE OR REPLACE TABLE
--     mesh_nct.barain_neoplasms_results
--     AS (SELECT * from barainNeoplasmsResults)

// COMMAND ----------

-- %sql
-- SELECT count(*) from mesh_nct.barain_neoplasms_results

// COMMAND ----------

// MAGIC %md
// MAGIC ## Analysis and Data VIZ

// COMMAND ----------

// MAGIC %md
// MAGIC Now first, let's take a look at the Brain Diseases sub-graph we synthesized from our fused graph. Since this is small graph, we will visualize this using the Python library networkx. One of the great benefits of doing all this on Spark, is that we can easily share information between scala (JVM) and Python, and even R.

// COMMAND ----------

// MAGIC %python
// MAGIC from matplotlib import pyplot as plt
// MAGIC import networkx as nx
// MAGIC import pandas as pd

// COMMAND ----------

// DBTITLE 1,Top 10 conditions 
// MAGIC %py
// MAGIC import plotly.express as px
// MAGIC top_100_conditions_pdf = sql("""
// MAGIC     SELECT cond, lower(condLabel) as condLabel, sum(numTrials) as sum_trials from mesh_nct.brain_diseases
// MAGIC         group by 1,2
// MAGIC         order by 3 desc
// MAGIC         limit 100
// MAGIC """).toPandas()
// MAGIC fig = px.bar(top_100_conditions_pdf[:10], x='condLabel', y='sum_trials')
// MAGIC fig.show()

// COMMAND ----------

// DBTITLE 1,Top 10 Interventions
// MAGIC %py
// MAGIC top_100_interventions_pdf = sql("""
// MAGIC     SELECT interv, lower(intervLabel) as intervLabel, sum(numTrials) as sum_trials from mesh_nct.brain_diseases
// MAGIC         group by 1,2
// MAGIC         order by 3 desc
// MAGIC         limit 100
// MAGIC """).toPandas()
// MAGIC fig = px.bar(top_100_interventions_pdf[:10], x='intervLabel', y='sum_trials')
// MAGIC fig.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Graph Visualization
// MAGIC Now let's take a look at the graph of top inetvention/conditions by number of trails:

// COMMAND ----------

// MAGIC %py
// MAGIC brain_diseases_gpdf = sql("""
// MAGIC     SELECT cond, lower(condLabel) as condLabel, numTrials as n_trials, interv, lower(intervLabel) as intervLabel
// MAGIC     from mesh_nct.brain_diseases
// MAGIC     order by numTrials desc
// MAGIC     limit 100
// MAGIC     """).dropDuplicates().toPandas()

// COMMAND ----------

// DBTITLE 1,Construct the network
// MAGIC %py
// MAGIC G=nx.Graph()
// MAGIC edge_list =[[m[1]['cond'],m[1]['interv'],m[1]['n_trials']] for m in brain_diseases_gpdf[['cond','interv','n_trials']].iterrows()]
// MAGIC G.add_weighted_edges_from(edge_list)

// COMMAND ----------

// DBTITLE 1,add labels
// MAGIC %python
// MAGIC labels = brain_diseases_gpdf[['cond', 'condLabel']].set_index('cond')['condLabel'].to_dict()
// MAGIC labels.update(brain_diseases_gpdf[['interv', 'intervLabel']].set_index('interv')['intervLabel'].to_dict())

// COMMAND ----------

// DBTITLE 1,visualize the network
// MAGIC %python
// MAGIC plt.figure(figsize=(20,10)) 
// MAGIC pos = nx.layout.bipartite_layout(G, set(brain_diseases_gpdf['cond']), scale=2, align='horizontal', aspect_ratio=2)
// MAGIC label_pos = {}
// MAGIC for cond in set(brain_diseases_gpdf['cond']):
// MAGIC     label_pos[cond] = pos[cond] + [0, -0.1]
// MAGIC for interv in set(brain_diseases_gpdf['interv']):
// MAGIC     label_pos[interv] = pos[interv] + [0, 0.1]
// MAGIC nx.draw(G, pos)
// MAGIC label_texts = nx.draw_networkx_labels(G, label_pos, labels=labels)
// MAGIC for cond in set(brain_diseases_gpdf['cond']):
// MAGIC     label_texts[cond].set_rotation(22)
// MAGIC for cond in set(brain_diseases_gpdf['interv']):
// MAGIC     label_texts[cond].set_rotation(22)
// MAGIC weights = nx.get_edge_attributes(G,'weight')
// MAGIC _ = nx.draw_networkx_edge_labels(G, pos, edge_labels=weights, font_color='r')

// COMMAND ----------

// MAGIC %md 
// MAGIC Here we see that most of the interventions have only one condition which they are connected. There is only one exception to this, [Galantamine](https://medlineplus.gov/druginfo/meds/a699058.html) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## TODO : add a SPARKQL query to fecth all trails with galantamine as intev and look at dates etc 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cond, lower(condLabel) as condLabel, numTrials as n_trials, interv, lower(intervLabel) as intervLabel
// MAGIC     from mesh_nct.brain_diseases
// MAGIC     where lower(intervLabel)='galantamine'

// COMMAND ----------

// MAGIC %md
// MAGIC ## TODO complete the query and visualize 

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC val query = """
// MAGIC     <galantamine interv from the graph>""".stripMargin
// MAGIC 
// MAGIC val results = triples.sparql(queryConfig, query).convertResults(Map())
// MAGIC ```
