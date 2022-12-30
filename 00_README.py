# Databricks notebook source
# MAGIC %md
# MAGIC #Creating a Knowledge Graph from MeSH and Clinical Trials
# MAGIC 
# MAGIC <img src="https://www.nlm.nih.gov/pubs/techbull/jf11/graphics/mesh_db_fig1.gif">
# MAGIC 
# MAGIC Knowledge Graphs are used to represent real-world entities and the relationships between them, and are often built by collecting and integrating data from a variety of sources.
# MAGIC An important step in creating a knowledge graph is Data Fusion. Data fusion is the process of combining data from multiple sources into a single, coherent representation. It is often used in situations where there are multiple data sources that are providing information about the same phenomenon, and the goal is to use this information to make more accurate inferences or decisions.
# MAGIC 
# MAGIC Data fusion can involve a variety of techniques, such as filtering, data cleaning, feature extraction, and machine learning. The specific approach used will depend on the nature of the data and the goals of the fusion process.
# MAGIC 
# MAGIC This process involves:
# MAGIC 
# MAGIC - Data collection: This involves gathering data from a variety of sources, such as databases, websites, and other online resources.
# MAGIC 
# MAGIC - Data cleaning and preprocessing: This involves cleaning and formatting the data so that it can be integrated into the knowledge graph. This may include tasks such as deduplication, entity resolution, and data standardization.
# MAGIC 
# MAGIC - Data integration: This involves integrating the data into the knowledge graph by creating nodes and edges that represent the entities and relationships in the data. This may also involve mapping data from different sources to a common schema or ontology, which defines the types of entities and relationships that can be represented in the knowledge graph.
# MAGIC 
# MAGIC - Data enrichment: This involves adding additional information to the knowledge graph by integrating data from additional sources or using machine learning algorithms to generate new insights.
# MAGIC 
# MAGIC Overall, data fusion in the context of knowledge graphs is a process of combining and integrating data from multiple sources in order to create a comprehensive and coherent representation of real-world entities and their relationships.
# MAGIC 
# MAGIC In this solution accelerator, we use [graphster library](https://github.com/wisecubeai/graphster), developed by [WiseCube](https://www.wisecube.ai/) to perform all the above mentioned steps and build a KG based on integrating clinical trails data with MeSH dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data 
# MAGIC [![](https://mermaid.ink/img/pako:eNptUEFqwzAQ_MqiUwtx6KUX0xgS26GFpLRJb1YPa2vtCGQpSHJCSfL3ynbS9lAdFjEzO7PMiVVGEItZrcyx2qH1sNpwDeHNC85f08ULrGn7zPnniC4CmiqpZYXqw0pUbtqYA8Tw-AAeS0UOIqjcYVi4GkXTKEkLgR5BmKNWBsXN7X9qJNOBTAvnsZG6eSptApXRtWw6i14a_atMZ7MkK-7W5HbQm5Xo6P6akfU2eTHgUHduXOyZceY9n92sBvWy2L7NN-8rGDJLi1KDkI6CqwOpPdkD6f4C93PCMoqS856sk84PW2MZ5-xvFJuwlmyLUoTGTz3Gmd9RS5zF4Suoxk55zri-BGm3DydTLqQ3lsV1qJomDDtvtl-6YrG3Hd1EmcTGYntVXb4BCZ-Wdw)](https://mermaid-js.github.io/mermaid-live-editor/edit/#pako:eNptUEFqwzAQ_MqiUwtx6KUX0xgS26GFpLRJb1YPa2vtCGQpSHJCSfL3ynbS9lAdFjEzO7PMiVVGEItZrcyx2qH1sNpwDeHNC85f08ULrGn7zPnniC4CmiqpZYXqw0pUbtqYA8Tw-AAeS0UOIqjcYVi4GkXTKEkLgR5BmKNWBsXN7X9qJNOBTAvnsZG6eSptApXRtWw6i14a_atMZ7MkK-7W5HbQm5Xo6P6akfU2eTHgUHduXOyZceY9n92sBvWy2L7NN-8rGDJLi1KDkI6CqwOpPdkD6f4C93PCMoqS856sk84PW2MZ5-xvFJuwlmyLUoTGTz3Gmd9RS5zF4Suoxk55zri-BGm3DydTLqQ3lsV1qJomDDtvtl-6YrG3Hd1EmcTGYntVXb4BCZ-Wdw)
# MAGIC 
# MAGIC ### MeSH Dataset
# MAGIC NCBI [MeSH](https://www.nlm.nih.gov/databases/download/mesh.html) (Medical Subject Headings) is a comprehensive controlled vocabulary for the purpose of indexing biomedical literature. The MeSH dataset is a collection of terms and definitions that describe the various topics and concepts in the field of biomedicine. These terms are organized into a hierarchical structure, with broader terms at the top and more specific terms at the bottom. The MeSH dataset is used by researchers, clinicians, and other healthcare professionals to search for and retrieve relevant articles and other resources from the biomedical literature. It is maintained by the National Center for Biotechnology Information (NCBI) at the National Institutes of Health (NIH). 
# MAGIC 
# MAGIC For building a knowledge graph, setting up the configuration is the foundation. Specifically, keeping all the data source specific, and schema specific references. Let's look at how we built the config for this knowledge graph.
# MAGIC 
# MAGIC ### Clinical Trials Data
# MAGIC To build a clinical trails knowledge graph, we fuse MeSH graph with clinical trials data, obtained from [ClinicalTrials.gov](https://aact.ctti-clinicaltrials.org/). This dataset which is regularly updated, contains information about all current and past trials for a given condition and the intervention that has been used. Note that this dataset is in tabular (csv) format. Using the [Configuration notebook]($./01_config) we setup parameters to be used for data fusion in the [Enrichment and Fusion notebook]($./03_enrichment_fusion).
# MAGIC 
# MAGIC ### Analysis
# MAGIC Finally we show how to [query]($./04_querying_derived_graph) the derived graph using [SPARQL](https://en.wikipedia.org/wiki/SPARQL) query language directly on databricks to gain insights for all trials for Brain Diseases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDF Format and Semantic Triples
# MAGIC 
# MAGIC We will building our graph in an RDF format. That means that everything in the graph is a triple. A triple is an encoding of a fact. Relationships between entities are expressed as `SUBJECT-PREDICATE-OBJECT`. The `SUBJECT` and `OBJECT` are nodes in our graph, and the `PREDICATE` is the relation or edge type in our graph. In RDF there are primarily three kinds of nodes. The first is the URI nodes which represent the actual entities. 
# MAGIC 
# MAGIC - `<http:data.com/id123>` A URI node has two parts, a namespace and a local ID. A namespace is a prefix that you can use to organize your entities into different types, or different sources. The local ID is the ID of that entity in that namespace. 
# MAGIC 
# MAGIC - The next type of node is the blank node which is used as a stand-in for a missing entity, or as a dummy node to create sophisticated subtructures in the graph. `_:123xyz` The blank node is repesented only by a unique ID. We will not be using blank nodes in this demo. 
# MAGIC 
# MAGIC - The third type of node is the literal node. Literals come themselves in two subtypes, **data literals** and **language literals**.
# MAGIC 
# MAGIC      - `"2021-08-18"^^<http://www.w3.org/2001/XMLSchema#date>` A data literal represents a number, boolean, or a string that does not come from a language (e.g. an ID). Data literals have two parts, the "lex" or "lexical form", which is the string representing the data, and the data type which is a URI that determines how the string should be interpreted. 
# MAGIC      - `"Leukemia"@en` Language literals represent a string from a human language. These also have two parts, the lex, and the language which is usually a ISO 2 Letter Language Code (e.g. en), but can also be specified to regions (e.g. en-gb)
# MAGIC   
# MAGIC There are many existing datasets that available in RDF format. The big difficulty in building knowledges in RDF (or even in other kinds of graphs), is merging different data sources, and making them available in a scalable way. So, let's look at how this library can help.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Graphster Library
# MAGIC [Graphster](https://github.com/wisecubeai/graphster) is an open-source knowledge graph library. It is a spark-based library purpose-built for scalable, end-to-end knowledge graph construction and querying from unstructured and structured source data. The graphster library takes a collection of documents, extracts mentions and relations to populate a raw knowledge graph, links mentions to entities in Wikidata, and then enriches the knowledge graph with facts from Wikidata. Once the knowledge graph is built, graphster can also help natively query the knowledge graph using SPARQL.
# MAGIC 
# MAGIC In this solution accelerator we showcase an end-to-end example of using this library to build a KG for clinical trials data.
# MAGIC <img src='https://www.wisecube.ai/wp-content/uploads/2022/10/graphster_architecture-1080x675.png'>

# COMMAND ----------

# MAGIC %md
# MAGIC ## License
# MAGIC Copyright / License info of the notebook. Copyright [2021] the Notebook Authors.  The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC |Library Name|Library License|Library License URL|Library Source URL|
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Pandas |BSD 3-Clause License| https://github.com/pandas-dev/pandas/blob/master/LICENSE | https://github.com/pandas-dev/pandas|
# MAGIC |Numpy |BSD 3-Clause License| https://github.com/numpy/numpy/blob/main/LICENSE.txt | https://github.com/numpy/numpy|
# MAGIC |Networkx |BSD license| https://github.com/networkx/networkx/blob/main/LICENSE.txt | https://github.com/networkx/networkx|
# MAGIC |Apache Spark |Apache License 2.0| https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark/tree/master/python/pyspark|
# MAGIC |Graphster |Apache License 2.0| https://github.com/wisecubeai/graphster/blob/master/LICENSE | https://github.com/wisecubeai/graphster|
# MAGIC 
# MAGIC 
# MAGIC |Author|
# MAGIC |-|
# MAGIC |Databricks Inc.|
# MAGIC |WiseCube AI|
# MAGIC 
# MAGIC 
# MAGIC ## Disclaimers
# MAGIC Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account.  Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
