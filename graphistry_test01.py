# Databricks notebook source
! pip install graphistry

# COMMAND ----------

#Optional: Uncomment - We find this speeds up calls 10%+ on some datasets
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

import graphistry  # if not yet available, install and/or restart Python kernel using the above

# To specify Graphistry account & server, use:
graphistry.register(api=3, username='amir', password=dbutils.secrets.get(scope="akermany-sec1 ", key="graphistry-psw"), protocol='https', server='hub.graphistry.com')
# For more options, see https://github.com/graphistry/pygraphistry#configure

graphistry.__version__

# COMMAND ----------

edges_table = "lipyeow_ctx.aad_edges_gold_day"
v = spark.sql(f"""
SELECT sub_id AS id, sub_name AS name
FROM {edges_table}
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT obj_id AS id, obj_name AS name
FROM {edges_table}
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT sub_id AS id, sub_name AS name
FROM lipyeow_ctx.same_as
""")

# duplicate the edges in the reverse direction in order to enable undirected path finding
e = spark.sql(f"""
SELECT distinct sub_id AS src, sub_name AS src_name, obj_id AS dst, obj_name AS dst_name, pred AS relationship
FROM {edges_table}
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
--UNION
--SELECT obj_id AS src, sub_name AS src_name, sub_id AS dst, obj_name AS dst_name, 'rev-' || pred AS relationship
--FROM solacc_cga.v_edges_day
--WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
--UNION
--SELECT sub_id AS src, sub_name AS src_name, obj_id AS dst, obj_name AS dst_name, pred AS relationship
--FROM solacc_cga.same_as
""")

#print(e.count())
display(e)

# COMMAND ----------

p = (graphistry
    .bind(point_title='name')
    .nodes(v, 'id')
    .bind(edge_title='relationship')
    .edges(e, 'src', 'dst')
    .settings(url_params={'strongGravity': 'true'})
    .plot()
)
p

# COMMAND ----------


