// Databricks notebook source
// MAGIC %md
// MAGIC # Creating a Knowledge Graph from MeSH and Clinical Trials
// MAGIC Now we are ready to create the KG by fusing clinical trials data and MeSH. In this notebook, we will use the transformers in the graphster library to enrich and fuse the data sets.

// COMMAND ----------

// DBTITLE 1,Imports
import com.graphster.orpheus.config.Configuration
import com.graphster.orpheus.data.datasets.{ClinicalTrials, MeSH}
import com.graphster.orpheus.enrichment.GraphLinker
import com.graphster.orpheus.fusion.{TripleExtractor, TripleMarker}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.SQLTransformer

import scala.language.postfixOps

import spark.implicits

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DATABASE mesh_nct

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's take a look at the number of triples in the graph before enrichment

// COMMAND ----------

// DBTITLE 1,count of triples before enrichment
// MAGIC %sql
// MAGIC select count(*) from mesh_nct.mesh_nct

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 1. Enrichment & Fusion
// MAGIC In order to turn data into knowledge, we need to add meaning, semantics. The data stored in a table does not have an automatic interpretation. Column names, and documentation can add this meaning, but these do not let us combine the knowledge that the table was built from, with other data. In order to synthesize new knowledge and create insights, we need to establish common elements between data sets. Storing this data in a single common schema allows researchers to speak a common language between different data sources with a shared understanding and semantics. Unfortunately, this semantic layer is often ignored in modern data lakes and is usually an afterthought.

// COMMAND ----------

// MAGIC %md
// MAGIC First let's run [config]($./01_config) notebook to get the config

// COMMAND ----------

// MAGIC %run ./01_config

// COMMAND ----------

// MAGIC %md
// MAGIC We will be merging in three tables with the MeSH data. The `studies` table contains details about clinical trials themselves, the `conditions` table contains information about the condition being studied, and the `interventions` table which is about the interventions in the trial.
// MAGIC 
// MAGIC Let's load these files that we will be transforming.

// COMMAND ----------

val studiesDF = spark.table("mesh_nct.studies")
val conditionsDF = spark.table("mesh_nct.conditions")
val interventionsDF = spark.table("mesh_nct.interventions")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Transform Studies
// MAGIC Now we build a pipeline to transform the `studies` table into a set of triples.
// MAGIC 
// MAGIC 1. `removeUntitled` - remove the trials with blank titles
// MAGIC 2. `nctType` - define the triple that assigns the type to the trial entities
// MAGIC 3. `nctFirstDate` - define the triple for the property that represents the trial submission date
// MAGIC 4. `nctBriefTitle` - define the triple that assigns the label (brief title)
// MAGIC 5. `nctOfficialTitle` - define the triple for the property that represents the trial official title

// COMMAND ----------

// DBTITLE 1,build the pipeline
// First we will remove the trials that are missing titles.
val removeUntitled = new SQLTransformer().setStatement(
  s"""
    |SELECT *
    |FROM __THIS__
    |WHERE ${config / "nct" / "title" / "brief_title" / "lex" / "column" getString} IS NOT NULL
    |AND ${config / "nct" / "title" / "official_title" / "lex" / "column" getString} IS NOT NULL
    |""".stripMargin
)

// This is the transformer that marks a column with metadata that define a triple that can be extract from the table
val nctType = new TripleMarker()
  .setInputCol("nct_id") // column to be marked
  .setTripleMeta(
    "nct_type", // name
    config / "nct" / "nct_uri" getConf, // SUBJECT
    config / "types" / "predicate" getConf, // PREDICATE
    config / "types" / "trial" getConf // OBJECT
  )

val nctFirstDate = new TripleMarker()
  .setInputCol("nct_id") // column to be marked
  .setTripleMeta(
    "nct_date", // name
    config / "nct" / "nct_uri" getConf, // SUBJECT
    config / "nct" / "date" / "predicate" getConf, // PREDICATE
    config / "nct" / "date" / "date" getConf // OBJECT
  )

val nctBriefTitle = new TripleMarker()
  .setInputCol("nct_id") // column to be marked
  .setTripleMeta(
    "nct_label", // name
    config / "nct" / "nct_uri" getConf, // SUBJECT
    config / "mesh" / "label_uri" getConf, // PREDICATE
    config / "nct" / "title" / "brief_title" getConf // OBJECT
  )

val nctOfficialTitle = new TripleMarker()
  .setInputCol("nct_id") // column to be marked
  .setTripleMeta(
    "nct_title", // name
    config / "nct" / "nct_uri" getConf, // SUBJECT
    config / "nct" / "title" / "predicate" getConf, // PREDICATE
    config / "nct" / "title" / "official_title" getConf // OBJECT
  )

val nctPipeline = new Pipeline().setStages(Array(
  removeUntitled,
  nctType,
  nctFirstDate,
  nctBriefTitle,
  nctOfficialTitle,
  new TripleExtractor() // This is the transformer that looks in the metadata for triples and extracts them all from the dataset
)).fit(studiesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Transform Conditions and Intervantions
// MAGIC We perform similar extraction the `conditions` and `interventions` tables.
// MAGIC 
// MAGIC 1. `condRemoveUnamed` - remove the conditions without names
// MAGIC 2. `condLinker` - link the condition into the MeSH graph using the `name` column and the `rdfs:label` property
// MAGIC 3. `condType` - define the triple that assigns the type to the condition
// MAGIC 4. `condLabel` - define the triple that assigns the label
// MAGIC 5. `condPred` - define the triple representing the relationship between the trial and the condition

// COMMAND ----------

// DBTITLE 1,conditions transformer
val condRemoveUnamed = new SQLTransformer().setStatement(
  s"""
    |SELECT *
    |FROM __THIS__
    |WHERE ${config / "nct" / "name" / "lex" / "column" getString} IS NOT NULL
    |""".stripMargin)

val condLinker = new GraphLinker()
  .setPropertyCol(config / "nct" / "name" / "lex" / "column" getString)
  .setPredicateCol(config / "mesh" / "label_uri" / "uri" / "value" getString)
  .setReferenceTable("mesh_nct.mesh")
  .setTransformation("LOWER($)")
  .setJoinType("leftouter")
  .setOutputCol("meshid")

val condType = new TripleMarker()
  .setInputCol(config / "nct" / "name" / "lex" / "column" getString)
  .setTripleMeta(
    "condition_type",
    config / "nct" / "condition" / "uri" getConf,
    config / "types" / "predicate" getConf,
    config / "types" / "condition" getConf
  )

val condLabel = new TripleMarker()
  .setInputCol(config / "nct" / "name" / "lex" / "column" getString)
  .setTripleMeta(
    "condition_label",
    config / "nct" / "condition" / "uri" getConf,
    config / "mesh" / "label_uri" getConf,
    config / "nct" / "name" getConf
  )

val condPred = new TripleMarker()
  .setInputCol("nct_id")
  .setTripleMeta(
    "condition",
    config / "nct" / "nct_uri" getConf,
    config / "nct" / "condition" / "predicate" getConf,
    config / "nct" / "condition" / "uri" getConf,
  )

val conditionPipeline = new Pipeline().setStages(Array(
  condRemoveUnamed,
  condLinker,
  condPred,
  condType,
  condLabel,
  new TripleExtractor()
)).fit(conditionsDF)

// COMMAND ----------

// DBTITLE 1,Interventions transformer
val intervRemoveUnamed = new SQLTransformer().setStatement(
  s"""
    |SELECT *
    |FROM __THIS__
    |WHERE ${config / "nct" / "name" / "lex" / "column" getString} IS NOT NULL
    |""".stripMargin)

val intervLinker = new GraphLinker()
  .setPropertyCol(config / "nct" / "name" / "lex" / "column" getString)
  .setPredicateCol(config / "mesh" / "label_uri" / "uri" / "value" getString)
  .setReferenceTable("mesh_nct.mesh")
  .setTransformation("LOWER($)")
  .setJoinType("leftouter")
  .setOutputCol("meshid")

val intervType = new TripleMarker()
  .setInputCol(config / "nct" / "name" / "lex" / "column" getString)
  .setTripleMeta(
    "intervention_type",
    config / "nct" / "intervention" / "uri" getConf,
    config / "types" / "predicate" getConf,
    config / "types" / "intervention" getConf
  )

val intervLabel = new TripleMarker()
  .setInputCol(config / "nct" / "name" / "lex" / "column" getString)
  .setTripleMeta(
    "intervention_label",
    config / "nct" / "intervention" / "uri" getConf,
    config / "mesh" / "label_uri" getConf,
    config / "nct" / "name" getConf
  )

val intervPred = new TripleMarker()
  .setInputCol("nct_id")
  .setTripleMeta(
    "condition",
    config / "nct" / "nct_uri" getConf,
    config / "nct" / "intervention" / "predicate" getConf,
    config / "nct" / "intervention" / "uri" getConf,
  )

val interventionPipeline = new Pipeline().setStages(Array(
  intervRemoveUnamed,
  intervLinker,
  intervPred,
  intervType,
  intervLabel,
  new TripleExtractor()
)).fit(interventionsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Build the Graph

// COMMAND ----------

val studyTriples = nctPipeline.transform(studiesDF).distinct()
val conditionTriples = conditionPipeline.transform(conditionsDF).distinct()
val interventionTriples = interventionPipeline.transform(interventionsDF).distinct()
val nctTriples = studyTriples.unionAll(conditionTriples).unionAll(interventionTriples).distinct()
val graph = spark.table("mesh_nct.mesh").unionAll(nctTriples).distinct()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now we have a graph that is the result of fusing MeSH with extracted data from clinical trials. These conditions and interventions in clinical trials, which were just strings associated to trial, now are connected to the millions of facts available in MeSH. Similarly, the knowledge already present in the MeSH knowledge graph, mainly hierarchical and type relationship, is now connected to real world facts.
// MAGIC 
// MAGIC Let's look at how many facts we found with this relatively simple set of pipelines.

// COMMAND ----------

println("Number of NCT study triples", studyTriples.count())
println("Number of NCT condition triples", conditionTriples.count())
println("Number of NCT intervention triples", interventionTriples.count())
println("Number of NCT triples", nctTriples.count())
println("Number of MeSH triples", spark.table("mesh_nct.mesh").count())
println("Number of total triples", graph.count())

// COMMAND ----------

// MAGIC %md
// MAGIC We have added **5.5 million** triples into the MeSH graph! Now, let us save this so we can begin querying.

// COMMAND ----------

graph.createOrReplaceTempView("allMeshNct")

// COMMAND ----------

// DBTITLE 1,Add the enriched data to database
// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE
// MAGIC     mesh_nct.mesh_nct
// MAGIC     AS (SELECT * from allMeshNct)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from mesh_nct.mesh_nct
