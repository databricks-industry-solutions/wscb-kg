// Databricks notebook source
// MAGIC %md
// MAGIC # Creating a Knowledge Graph from MeSH and Clinical Trials

// COMMAND ----------

// MAGIC %md
// MAGIC ## Data Download
// MAGIC 
// MAGIC This notebook will go through the download and staging of the data we will be using. The graphster library includes a module for downloading data sets. This module has both MeSH and clinical trials download utilities. 
// MAGIC 
// MAGIC We will stage the data as tables. The MeSH data already comes in a graph format, NTriples. The clinical trials data comes as a collection of pipe-seperated-values files. We will create tables from the MeSH file as well as each clinical trials file.

// COMMAND ----------

// DBTITLE 1,Imports
import com.graphster.orpheus.config.Configuration
import com.graphster.orpheus.data.datasets.{ClinicalTrials, MeSH}

import java.io.File
import spark.implicits

// COMMAND ----------

val user=sql("select current_user() as user").collect()(0)(0)
val dataPath = "/home/" + user + "/data/mesh_kg/"
val deltaPath = dataPath + "delta/"

// COMMAND ----------

dbutils.fs.mkdirs(deltaPath)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Add MeSH
// MAGIC Now we add MeSH dataset

// COMMAND ----------

// DBTITLE 1,create mesh schema
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS mesh_nct

// COMMAND ----------

// MAGIC %md
// MAGIC Download MeSH data and create a dataframe

// COMMAND ----------

val filepath: String = MeSH.download()
dbutils.fs.mv("file:" + filepath, dataPath + "mesh.nt")
val meshDF = MeSH.load(dataPath + "mesh.nt")
meshDF.createOrReplaceTempView("allMeshNct")

// COMMAND ----------

// DBTITLE 1,save mesh data
// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE
// MAGIC     mesh_nct.mesh_nct
// MAGIC     AS (SELECT * from allMeshNct)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DATABASE mesh_nct

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TABLES In mesh_nct

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from mesh_nct.mesh
// MAGIC limit 10

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Add Clinical Trials
// MAGIC 
// MAGIC The clinical trials data is more complicated. It comes as a set of 50 CSVs (pipe separated). Here we will just loop over each file, and create the table.

// COMMAND ----------

// DBTITLE 1,download and add clinical trials data
if (!spark.catalog.listTables("mesh_nct").collect().map(_.name).contains("conditions")) {
  val zipfilePath = ClinicalTrials.download()
  val directory = ClinicalTrials.unzip(zipfilePath)
  val dbfsDirectory = dataPath+"aact"
  dbutils.fs.mkdirs(dbfsDirectory)
  
  ClinicalTrials.getFiles(directory).foreach(println)
  ClinicalTrials.getFiles(directory).foreach {
    filename =>
      val dbfsPath = s"$dbfsDirectory/$filename"
      if (!dbutils.fs.ls(dbfsDirectory).contains(filename)) {
        dbutils.fs.cp(s"file:/databricks/driver/aact/$filename", dbfsPath)
      }
      val tablename = filename.replace(".txt", "")
      val df = spark.read.option("inferSchema", true).option("delimiter", "|").option("header", true).csv(dbfsPath)
      if (!spark.catalog.listTables("mesh_nct").collect().map(_.name).contains(tablename)) {
        df.write.format("delta").saveAsTable(s"mesh_nct.$tablename")
      }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC We will be merging in three tables with the MeSH data. The `studies` table contains details about clinical trials themselves, the `conditions` table contains information about the condition being studied, and the `interventions` table which is about the interventions in the trial.

// COMMAND ----------

println("mesh triples", spark.table("mesh_nct.mesh").count())
println("NCT studies", spark.table("mesh_nct.studies").count())
println("NCT conditions", spark.table("mesh_nct.conditions").count())
println("NCT interventions", spark.table("mesh_nct.interventions").count())

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from mesh_nct.studies
// MAGIC limit 10
