// Databricks notebook source
// MAGIC %md
// MAGIC #Creating a Knowledge Graph from MeSH and Clinical Trials
// MAGIC 
// MAGIC ## Data
// MAGIC NCBI [MeSH](https://www.nlm.nih.gov/databases/download/mesh.html) (Medical Subject Headings) is a comprehensive controlled vocabulary for the purpose of indexing biomedical literature. The MeSH dataset is a collection of terms and definitions that describe the various topics and concepts in the field of biomedicine. These terms are organized into a hierarchical structure, with broader terms at the top and more specific terms at the bottom. The MeSH dataset is used by researchers, clinicians, and other healthcare professionals to search for and retrieve relevant articles and other resources from the biomedical literature. It is maintained by the National Center for Biotechnology Information (NCBI) at the National Institutes of Health (NIH). 
// MAGIC 
// MAGIC ## Writing the config
// MAGIC 
// MAGIC For building a knowledge graph, setting up the configuration is the foundation. Specifically, keeping all the data source specific, and schema specific references. Let's look at how we built the config for this knowledge graph.
// MAGIC 
// MAGIC First let's talk about our graph format RDF.
// MAGIC 
// MAGIC ### Semantic Triples
// MAGIC 
// MAGIC We will building our graph in an RDF format. That means that everything in the graph is a triple. A triple is an encoding of a fact. Relationships between entities are expressed as `SUBJECT-PREDICATE-OBJECT`. The `SUBJECT` and `OBJECT` are nodes in our graph, and the `PREDICATE` is the relation or edge type in our graph. In RDF there are primarily three kinds of nodes. The first is the URI nodes which represent the actual entities. 
// MAGIC 
// MAGIC - `<http:data.com/id123>` A URI node has two parts, a namespace and a local ID. A namespace is a prefix that you can use to organize your entities into different types, or different sources. The local ID is the ID of that entity in that namespace. 
// MAGIC 
// MAGIC - The next type of node is the blank node which is used as a stand-in for a missing entity, or as a dummy node to create sophisticated subtructures in the graph. `_:123xyz` The blank node is repesented only by a unique ID. We will not be using blank nodes in this demo. 
// MAGIC 
// MAGIC - The third type of node is the literal node. Literals come themselves in two subtypes, **data literals** and **language literals**.
// MAGIC 
// MAGIC      - `"2021-08-18"^^<http://www.w3.org/2001/XMLSchema#date>` A data literal represents a number, boolean, or a string that does not come from a language (e.g. an ID). Data literals have two parts, the "lex" or "lexical form", which is the string representing the data, and the data type which is a URI that determines how the string should be interpreted. 
// MAGIC      - `"Leukemia"@en` Language literals represent a string from a human language. These also have two parts, the lex, and the language which is usually a ISO 2 Letter Language Code (e.g. en), but can also be specified to regions (e.g. en-gb)
// MAGIC   
// MAGIC There are many existing datasets that available in RDF format. The big difficulty in building knowledges in RDF (or even in other kinds of graphs), is merging different data sources, and making them available in a scalable way. So, let's look at how this library can help.

// COMMAND ----------

// MAGIC %md
// MAGIC Add a description of graphster library:
// MAGIC 
// MAGIC <img src='https://www.wisecube.ai/wp-content/uploads/2022/10/graphster_architecture-1080x675.png'>

// COMMAND ----------


