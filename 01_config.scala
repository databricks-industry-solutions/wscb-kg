// Databricks notebook source
// MAGIC %md
// MAGIC # Creating a Knowledge Graph from MeSH and Clinical Trials: Config
// MAGIC In this notebook we set up the initial configurations for our knowledge graph.

// COMMAND ----------

// DBTITLE 1,Import libraires
import com.graphster.orpheus.config.Configuration
import com.graphster.orpheus.config.graph._
import com.graphster.orpheus.config.table._
import com.graphster.orpheus.config.types.MetadataField
import com.graphster.orpheus.query.config._

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Define entities to be extracted
// MAGIC 
// MAGIC In this demo we will use all of MeSH, so we don't need to worry about extracting specific data. 
// MAGIC 
// MAGIC For the clinical trials, we need to have 
// MAGIC 
// MAGIC 1. the trial
// MAGIC 2. the condition
// MAGIC 3. the intervention. 
// MAGIC 
// MAGIC So let's start with determining the types we will need.

// COMMAND ----------

val typeConfig = Configuration(
  "predicate" -> MetadataField(URIGraphConf("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")),
  "trial" -> MetadataField(URIGraphConf("http://schema.org/MedicalTrial")),
  "condition" -> MetadataField(URIGraphConf("http://schema.org/MedicalCondition")),
  "intervention" -> MetadataField(URIGraphConf("http://schema.org/MedicalProcedure")),
)

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's take a look at our graph configurations

// COMMAND ----------

println(typeConfig.yaml)

// COMMAND ----------

// MAGIC %md
// MAGIC These will all be entities in our graph, as well as the the predicate we will use to assign the type to them. Notice that these type entities are not from our two data sets. Sometimes, you may be faced with the need for an entity that is not contained data sets. If you are in control of the namespaces, you can just add what you need. However, in our situation we do not control the MeSH namespace. The first step should be to look at the w3 schemas, and schema.org - where we got these.
// MAGIC 
// MAGIC For the trials, we will need to construct a URI format that will represent them as entities in the graph. In RDF, we want the URIs to resolve to some page concerning the entity. If you are managing your own namespace, you may need to maintain a large set of entity profile pages or an API that generates them. Here we will use the URL for the clinical trial - `http://clinicaltrials.gov/ct2/show/...`.

// COMMAND ----------

val nctURIConfig = URIGraphConf(ConcatValueConf(Seq(
  StringValueConf("http://clinicaltrials.gov/ct2/show/"),
  ColumnValueConf("nct_id")
)))

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2. Data Fusion
// MAGIC 
// MAGIC Here we need to decide what entities we need to match. For us, we know the trials do not occur in MeSH, so we will try and match the conditions and interventions. Neither of which are guaranteed to be in MeSH, on top of the reality that there will certainly missed matches. But dealing with non-matches come later. Now we need to consider what properties we have in the two data sets for the entities. The ideal situation is where there is an explicit and unambiguous identifier shared between the two data sets. That is not the case here. The tables we are working with in the clinical trials data only have names. This means that we need to join to a name in MeSH. In the MeSH graph the main name of an entity is represented with a triple using the `rdfs:label` predicate. So we know we will be using that predicate in MeSH.
// MAGIC 
// MAGIC On the clinical trials side, we need to mark the fields that we will use to match against the property. Both the conditions and interventions tables use the same field name.

// COMMAND ----------

val meshConfig = Configuration("label_uri" -> MetadataField(URIGraphConf("http://www.w3.org/2000/01/rdf-schema#label")))
val nameConfig = LangLiteralGraphConf(ColumnValueConf("name"), StringValueConf("en"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 3. Property Extraction
// MAGIC 
// MAGIC As with the entities, we are not extracting properties from MeSH, as we are fusing data from clinical trials into the MeSH graph.
// MAGIC 
// MAGIC Of the three types we are extracting, two are being matched into MeSH. This means that they have properties already associated with them. So let's focus on the properties for the trials.
// MAGIC 
// MAGIC For each of these properties, we need the following
// MAGIC 
// MAGIC 1. source column or a transformation on source data
// MAGIC 2. the predicate to represent it
// MAGIC    - In this situation, we are not filling in missing properties for extant entities in MeSH, so our predicates will need to be supplied externally, specifically from schema.org
// MAGIC    
// MAGIC The title of the trial can serve as a `rdfs:label`, however there are two titles available - `official_title` and `brief_title`. We can use `brief_title` as the label, but we want to keep the _proper_ name as well. We already have the predicate for `brief_title` defined above from MeSH. The `official_title` will use the `title` predicate from schema.org.

// COMMAND ----------

val titleConfig = Configuration(
  "predicate" -> MetadataField(URIGraphConf("http://schema.org/title")),
  "official_title" -> MetadataField(LangLiteralGraphConf(ColumnValueConf("official_title"), StringValueConf("en"))),
  "brief_title" -> MetadataField(LangLiteralGraphConf(ColumnValueConf("brief_title"), StringValueConf("en"))),
)

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's do the config for the date of the trial.
// MAGIC 
// MAGIC There are many dates to choose from. In this example we will be looking at trends. Since a clinical trial can take a long time, we are more interested in the start date, than the end date. So we will use the `study_first_submitted_date`. We will also need to transform it into the string format that RDF can use, so we will encode that here. Putting basic transformations is an option, not a necessity. The benefit here is that it keeps us from needing to bloat the main pipeline.

// COMMAND ----------

val dateConfig = Configuration(
      "predicate" -> MetadataField(URIGraphConf("http://schema.org/startDate")),
      "date" -> MetadataField(DataLiteralGraphConf(ColumnValueConf("DATE_FORMAT(study_first_submitted_date, 'yyyy-MM-dd')"), "date"))
    )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The next step is additional transformations we need before making the graph. This will be use case specific. For this notebook, we mainly are concerned with having fallback values for failed matches.
// MAGIC 
// MAGIC If there is a match, then we will store the identified MeSH entity in the column `meshid`. If not, we need to construct another URI. Unfortunately, we don't have any more clever tricks for making resolvable URIs, specifically, because we have no non-clinical trials identifier. So we will make "fake" URI (i.e. one that does not resolve). We will make this with the ID from the clinical trials tables as the local value, and a made up namespace.

// COMMAND ----------

val conditionConfig = Configuration(
  "predicate" -> MetadataField(URIGraphConf("http://schema.org/healthCondition")),
  "uri" -> MetadataField(URIGraphConf(FallbackValueConf(
    Seq(ColumnValueConf("meshid")),
    ConcatValueConf(Seq(
        StringValueConf("http://wisecube.com/condition#AACT"),
        ColumnValueConf("id"),
      ))))))

val interventionConfig = Configuration(
  "predicate" -> MetadataField(URIGraphConf("http://schema.org/studySubject")),
  "uri" -> MetadataField(URIGraphConf(FallbackValueConf(
    Seq(ColumnValueConf("meshid")),
    ConcatValueConf(
      Seq(
        StringValueConf("http://wisecube.com/intervention#AACT"),
        ColumnValueConf("id"),
      ))))))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Finally, we need to store the information we will use for querying. Primarily, this is used to set a list of default prefixes (namespaces) for use in SPARQL. However, we can also pass other Bellman specific configs. More on this when we get to querying.

// COMMAND ----------

val queryConfig = QueryConfig(
  Prefixes(Map(
    "rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "rdfs" -> "<http://www.w3.org/2000/01/rdf-schema#>",
    "schema" -> "<http://schema.org/>",
    "" -> "<http://id.nlm.nih.gov/mesh/>",
    "mv" -> "<http://id.nlm.nih.gov/mesh/vocab#>",
    "nct" -> "<http://clinicaltrials.gov/ct2/show/>",
    "cond" -> "<http://wisecube.com/condition#>",
    "interv" -> "<http://wisecube.com/intervention#>",
  )), 
  formatRdfOutput = false
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now that we have created these configs, let's gather them all together, We will namespace them by data set and purpose.

// COMMAND ----------

val config = Configuration(
  "types" -> MetadataField(typeConfig),
  "mesh" -> MetadataField(meshConfig),
  "nct" -> MetadataField(Configuration(
    "name" -> MetadataField(nameConfig),
    "nct_uri" -> MetadataField(nctURIConfig),
    "title" -> MetadataField(titleConfig),
    "date" -> MetadataField(dateConfig),
    "condition" -> MetadataField(conditionConfig),
    "intervention" -> MetadataField(interventionConfig)
  )),
  "query" -> MetadataField(queryConfig)
)

// COMMAND ----------

config.yaml
