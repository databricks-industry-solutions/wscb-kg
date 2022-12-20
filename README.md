![image](https://user-images.githubusercontent.com/86326159/206014015-a70e3581-e15c-4a10-95ef-36fd5a560717.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)


# Creating a Knowledge Graph from MeSH and Clinical Trials

## Data
NCBI [MeSH](https://www.nlm.nih.gov/databases/download/mesh.html) (Medical Subject Headings) is a comprehensive controlled vocabulary for the purpose of indexing biomedical literature. The MeSH dataset is a collection of terms and definitions that describe the various topics and concepts in the field of biomedicine. These terms are organized into a hierarchical structure, with broader terms at the top and more specific terms at the bottom. The MeSH dataset is used by researchers, clinicians, and other healthcare professionals to search for and retrieve relevant articles and other resources from the biomedical literature. It is maintained by the National Center for Biotechnology Information (NCBI) at the National Institutes of Health (NIH). 

## Writing the config

For building a knowledge graph, setting up the configuration is the foundation. Specifically, keeping all the data source specific, and schema specific references. Let's look at how we built the config for this knowledge graph.

First let's talk about our graph format RDF.

### Semantic Triples

We will building our graph in an RDF format. That means that everything in the graph is a triple. A triple is an encoding of a fact. Relationships between entities are expressed as `SUBJECT-PREDICATE-OBJECT`. The `SUBJECT` and `OBJECT` are nodes in our graph, and the `PREDICATE` is the relation or edge type in our graph. In RDF there are primarily three kinds of nodes. The first is the URI nodes which represent the actual entities. 

- `<http:data.com/id123>` A URI node has two parts, a namespace and a local ID. A namespace is a prefix that you can use to organize your entities into different types, or different sources. The local ID is the ID of that entity in that namespace. 

- The next type of node is the blank node which is used as a stand-in for a missing entity, or as a dummy node to create sophisticated subtructures in the graph. `_:123xyz` The blank node is repesented only by a unique ID. We will not be using blank nodes in this demo. 

- The third type of node is the literal node. Literals come themselves in two subtypes, **data literals** and **language literals**.

     - `"2021-08-18"^^<http://www.w3.org/2001/XMLSchema#date>` A data literal represents a number, boolean, or a string that does not come from a language (e.g. an ID). Data literals have two parts, the "lex" or "lexical form", which is the string representing the data, and the data type which is a URI that determines how the string should be interpreted. 
     - `"Leukemia"@en` Language literals represent a string from a human language. These also have two parts, the lex, and the language which is usually a ISO 2 Letter Language Code (e.g. en), but can also be specified to regions (e.g. en-gb)
  
There are many existing datasets that available in RDF format. The big difficulty in building knowledges in RDF (or even in other kinds of graphs), is merging different data sources, and making them available in a scalable way. So, let's look at how this library can help.

___

<amir.kermany@databricks.com>

___

<img src='https://www.wisecube.ai/wp-content/uploads/2022/10/graphster_architecture-1080x675.png'>

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| graphster                                 | Knowledge graph construction and querying      | Apache-2        | https://github.com/wisecubeai/graphster                      |

## Getting started

Although specific solutions can be downloaded as .dbc archives from our websites, we recommend cloning these repositories onto your databricks environment. Not only will you get access to latest code, but you will be part of a community of experts driving industry best practices and re-usable solutions, influencing our respective industries. 

<img width="500" alt="add_repo" src="https://user-images.githubusercontent.com/4445837/177207338-65135b10-8ccc-4d17-be21-09416c861a76.png">

To start using a solution accelerator in Databricks simply follow these steps: 

1. Clone solution accelerator repository in Databricks using [Databricks Repos](https://www.databricks.com/product/repos)
2. Attach the `RUNME` notebook to any cluster and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. The job configuration is written in the RUNME notebook in json format. 
3. Execute the multi-step-job to see how the pipeline runs. 
4. You might want to modify the samples in the solution accelerator to your need, collaborate with other users and run the code samples against your own data. To do so start by changing the Git remote of your repository  to your organization’s repository vs using our samples repository (learn more). You can now commit and push code, collaborate with other user’s via Git and follow your organization’s processes for code development.

The cost associated with running the accelerator is the user's responsibility.


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
