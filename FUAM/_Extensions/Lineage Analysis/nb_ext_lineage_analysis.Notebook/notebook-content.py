# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "52a41b90-67fd-466f-b1b7-0173d4ad947a",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "248ec193-1176-49d4-ba36-ced311d1e3c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "52a41b90-67fd-466f-b1b7-0173d4ad947a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Lineage Analysis
# 
# This notebook will take inventory data collected by FUAM and locate parent - child relationships for many Fabric items.
# 
# The resulting schema of each Fabric item is:
# ```
# root
#  |-- DownStreamId: string (nullable = true)
#  |-- DownStreamWorkspaceId: string (nullable = true)
#  |-- DownStreamType: string (nullable = false)
#  |-- UpStreamId: string (nullable = true)
#  |-- UpStreamWorkspaceId: string (nullable = true)
#  |-- UpStreamType: string (nullable = false)
#  |-- SourceDF: string (nullable = false)
#  ```
# 
#  Each Fabric item dataframe is unioned together for further analysis.


# MARKDOWN ********************

# ##### Imports, Find Base File, & Create Base DFs

# CELL ********************

from pyspark.sql.functions import explode, explode_outer, col, concat_ws, sha2, lit, upper, coalesce, concat, row_number, udf
from pyspark.sql import Window
import networkx as nx

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_path = "abfss://FUAM@onelake.dfs.fabric.microsoft.com/FUAM_Lakehouse.Lakehouse/Files/history/inventory"
path = notebookutils.fs.ls(base_path)

while all(file.isDir for file in path):
    max_dir = max(path, key = lambda f: f.name).name
    print(f"Found directory -> {max_dir}")
    base_path += f"/{max_dir}"
    path = notebookutils.fs.ls(base_path)
print(f"Max Directory -> {base_path}")

df = spark.read.json(base_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_model_df = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    "*",
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.datasets")).alias("SemanticModel")
).select(
    col("WorkspaceId"),
    col("WorkspaceName"),
    col("SemanticModel.id").alias("SemanticModelId"),
    col("SemanticModel.name").alias("SemanticModelName"),
    col("SemanticModel")
)
# base_model_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Create Sections
# 
# Match the below schema.
# 
# ```
# root
#  |-- DownStreamId: string (nullable = true)
#  |-- DownStreamWorkspaceId: string (nullable = true)
#  |-- DownStreamType: string (nullable = false)
#  |-- UpStreamId: string (nullable = true)
#  |-- UpStreamWorkspaceId: string (nullable = true)
#  |-- UpStreamType: string (nullable = false)
#  |-- SourceDF: string (nullable = false)
# ```

# CELL ********************

sql_to_rel = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    "*",
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.SQLAnalyticsEndpoint")).alias("SQLAnalyticsEndpoint")
).select(
    col("Workspace.id").alias("DownStreamWorkspaceId"),
    col("SQLAnalyticsEndpoint.id").alias("DownStreamId"),
    explode(col("SQLAnalyticsEndpoint.relations")).alias("relations")
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    lit("SQLAnalyticsEndpoint").alias("DownStreamType"),
    col("relations.dependentOnArtifactId").alias("UpStreamId"),
    col("relations.workspaceId").alias("UpStreamWorkspaceId"),
    lit("Relation").alias("UpStreamType"),
    lit("sql_to_rel").alias("SourceDF")
)

sql_to_rel.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_to_rel = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    "*",
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.Lakehouse")).alias("Lakehouse")
).select(
    col("Workspace.id").alias("DownStreamWorkspaceId"),
    col("Lakehouse.id").alias("DownStreamId"),
    explode(col("Lakehouse.relations")).alias("relations")
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    lit("Lakehouse").alias("DownStreamType"),
    col("relations.dependentOnArtifactId").alias("UpStreamId"),
    col("relations.workspaceId").alias("UpStreamWorkspaceId"),
    lit("Relation").alias("UpStreamType"),
    lit("lh_to_rel").alias("SourceDF")
)

lh_to_rel.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb_to_rel = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    "*",
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.Notebook")).alias("Notebook")
).select(
    col("Workspace.id").alias("DownStreamWorkspaceId"),
    col("Notebook.id").alias("DownStreamId"),
    explode(col("Notebook.relations")).alias("relations")
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    lit("Notebook").alias("DownStreamType"),
    col("relations.dependentOnArtifactId").alias("UpStreamId"),
    col("relations.workspaceId").alias("UpStreamWorkspaceId"),
    lit("Relation").alias("UpStreamType"),
    lit("nb_to_rel").alias("SourceDF")
)

nb_to_rel.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm_to_sm = base_model_df.select(
    col("SemanticModelId").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    explode_outer(col("SemanticModel.upstreamDatasets")).alias("UpStreamDataset")
).select(
    "DownStreamId",
    col("DownStreamWorkspaceId"),
    lit("SemanticModel").alias("DownStreamType"),
    col("UpStreamDataset.targetDatasetId").alias("UpStreamId"),
    col("UpStreamDataset.groupId").alias("UpStreamWorkspaceId"),
    lit("SemanticModel").alias("UpStreamType"),
    lit("sm_to_sm").alias("SourceDF")
).where(col("UpStreamId").isNotNull())

sm_to_sm.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm_to_rel = base_model_df.select(
    col("SemanticModelId").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    explode_outer(col("SemanticModel.relations")).alias("UpStreamRelation")
).select(
    "DownStreamId",
    col("DownStreamWorkspaceId"),
    lit("SemanticModel").alias("DownStreamType"),
    col("UpStreamRelation.dependentOnArtifactId").alias("UpStreamId"),
    col("UpStreamRelation.workspaceId").alias("UpStreamWorkspaceId"),
    lit("Relation").alias("UpStreamType"),
    lit("sm_to_sm").alias("SourceDF")
).where(col("UpStreamId").isNotNull())

sm_to_rel.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm_to_df = base_model_df.select(
    col("SemanticModelId").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    explode_outer(col("SemanticModel.upstreamDataflows")).alias("UpStreamDataflow")
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    lit("SemanticModel").alias("DownStreamType"),
    col("UpStreamDataflow.targetDataflowId").alias("UpStreamId"),
    col("UpStreamDataflow.groupId").alias("UpstreamWorkspaceId"),
    lit("Dataflow").alias("UpStreamType"),
    lit("sm_to_df").alias("SourceDF")
).where(col("UpStreamId").isNotNull())
sm_to_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm_to_source = base_model_df.select(
    col("SemanticModelId").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    explode(col("SemanticModel.datasourceUsages")).alias("datasourceUsages")
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    lit("SemanticModel").alias("DownStreamType"),
    col("datasourceUsages.datasourceInstanceId").alias("UpStreamId"),
    lit("DATASOURCE").alias("UpstreamWorkspaceId"),
    lit("DataSource").alias("UpStreamType"),
    lit("sm_to_source").alias("SourceDF")
).where(col("UpStreamId").isNotNull())
sm_to_source.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm = spark.read.table("semantic_models").select("WorkspaceId", "SemanticModelId")
r_to_sm = spark.read.table("reports").where(col("AppId").isNull()).where(col("ReportType") == "PowerBIReport").select(
    col("ReportId").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("Report").alias("DownStreamType"),
    col("SemanticModelId").alias("UpStreamId"),
    # col("SemanticModelWorkspaceId").alias("UpStreamWorkspaceId"),
    lit("SemanticModel").alias("UpStreamType"),
    lit("r_to_sm").alias("SourceDF")
).join(
    sm, col("UpStreamId") == col("SemanticModelId"), "leftouter"
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    "DownStreamType",
    "UpStreamId",
    col("WorkspaceId").alias("UpStreamWorkspaceId"),
    "UpStreamType",
    "SourceDF"
).where(col("UpStreamId").isNotNull())

r_to_sm.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

app_to_r = spark.read.table("reports").where(col("AppId").isNotNull()).where(col("ReportType") == "PowerBIReport").select(
    col("ReportId").alias("DownStreamId"), #App Report
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("ReportApp").alias("DownStreamType"),
    col("OriginalReportObjectId").alias("UpStreamId"),
    col("WorkspaceId").alias("UpStreamWorkspaceId"),
    lit("Report").alias("UpStreamType"),
    lit("app_to_r")
)

app_to_r.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Handle the dataflow nonsense in backend API
dataflow = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode_outer(col("Workspace.dataflows")).alias("dataflows")
).select(
    "WorkspaceId",
    col("dataflows.objectid").alias("id"),
    col("dataflows.datasourceUsages"),
    col("dataflows.relations")
)

dataflow_cicd = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode_outer(col("Workspace.Dataflow")).alias("dataflows")
).select(
    "WorkspaceId",
    col("dataflows.id").alias("id"),
    col("dataflows.datasourceUsages"),
    col("dataflows.relations")
)
dataflow_combined = dataflow.union(dataflow_cicd)
dataflow_combined.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_to_df = dataflow_combined.select(
    "WorkspaceId",
    "id",
    explode_outer(col("dataSourceUsages")).alias("DataSource")
).select(
    col("id").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("Dataflow").alias("DownStreamType"),
    col("Datasource.datasourceInstanceId").alias("UpStreamId"),
    lit(None).alias("UpStreamWorkspaceId"),
    lit("DataSource").alias("UpStreamType"),
    lit("source_to_df").alias("SourceDF")
)

source_to_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_to_dest = dataflow_combined.select(
    col("WorkspaceId").alias("UpStreamWorkspaceId"),
    col("id").alias("UpStreamId"),
    explode_outer(col("relations")).alias("destination")
).select(
    col("destination.dependentOnArtifactId").alias("DownStreamId"),
    col("destination.workspaceId").alias("DownStreamWorkspaceId"),
    lit("Destination").alias("DownStreamType"),
    "UpStreamId",
    "UpStreamWorkspaceId",
    lit("Dataflow").alias("UpStreamType"),
    lit("df_to_dest").alias("SourceDF")
)
df_to_dest.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_to_dp = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.DataPipeline")).alias("DataPipeline")
).select(
    "WorkspaceId",
    "WorkspaceName",
    col("DataPipeline.id"),
    explode_outer(col("DataPipeline.DataSourceUsages")).alias("DataSourceUsages")
).select(
    col("id").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("DataPipeline").alias("DownStreamType"),
    col("DataSourceUsages.datasourceInstanceId").alias("UpStreamId"),
    lit(None).alias("UpStreamWorkspaceId"),
    lit("DataSource").alias("UpStreamType"),
    lit("source_to_dp").alias("SourceDF")
)

source_to_dp.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dp_to_rel = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.DataPipeline")).alias("DataPipeline")
).select(
    "WorkspaceId",
    "WorkspaceName",
    col("DataPipeline.id"),
    explode_outer(col("DataPipeline.relations")).alias("relations")
).select(
    col("id").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("DataPipeline").alias("DownStreamType"),
    col("relations.dependentOnArtifactId").alias("UpStreamId"),
    col("relations.workspaceId").alias("UpStreamWorkspaceId"),
    lit(None).alias("UpStreamType"),
    lit("dp_to_rel").alias("SourceDF")
)

dp_to_rel.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

staging_lakehouse = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.Lakehouse")).alias("Lakehouse")
).where("Lakehouse.name like 'StagingLakehouseForDataflows%'").select(
    col("Lakehouse.id").alias("UpStreamId"),
    col("WorkspaceId").alias("UpStreamWorkspaceId"),
    lit("Lakehouse").alias("UpStreamType")
)

lakehouse_end = df.select(
    explode(col("workspaces")).alias("Workspace")
).select(
    col("Workspace.name").alias("WorkspaceName"),
    col("Workspace.id").alias("WorkspaceId"),
    explode(col("Workspace.Lakehouse")).alias("Lakehouse")
).where("Lakehouse.name not like 'StagingLakehouseForDataflows%' and Lakehouse.name != 'DataflowsStagingLakehouse'").select(
    col("Lakehouse.id").alias("DownStreamId"),
    col("WorkspaceId").alias("DownStreamWorkspaceId"),
    lit("Lakehouse").alias("DownStreamType")
)



lh_to_lh = staging_lakehouse.join(
    lakehouse_end, 
    staging_lakehouse.UpStreamWorkspaceId == lakehouse_end.DownStreamWorkspaceId,
    'left_outer'
).select(
    "DownStreamId",
    "DownStreamWorkspaceId",
    "DownStreamType",
    "UpStreamId",
    "UpStreamWorkspaceId",
    "UpStreamType",
    lit("lh_to_lh").alias("SourcDF")
).where(col("DownStreamId").isNotNull())

lh_to_lh.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Union It All

# CELL ********************

hierarchy_df = sql_to_rel.union(
  nb_to_rel).union(
  lh_to_rel).union(
  sm_to_sm).union(
  sm_to_rel).union(
  r_to_sm).union(
  app_to_r).union(
  sm_to_source).union(
  sm_to_df).union(
  df_to_dest).union(
  source_to_df).union(
  source_to_dp).union(
  dp_to_rel).union(
  lh_to_lh).select(
  upper(col("DownStreamId")).alias("DownStreamId"),
  upper(col("DownStreamWorkspaceId")).alias("DownStreamWorkspaceId"),
  col("DownStreamType"),
  upper(col("UpStreamId")).alias("UpStreamId"),
  upper(col("UpStreamWorkspaceId")).alias("UpStreamWorkspaceId"),
  col("UpStreamType"),
  col("SourceDF") 
)
hierarchy_df.createOrReplaceTempView("hierarchy_test")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hierarchy = spark.sql("""   
select
    distinct
    h.UpStreamId,
    h.UpStreamWorkspaceId as UpStreamWorkspaceId,
    coalesce( a_up.name,  d_up.`connectionDetails.path`,  d_up.`connectionDetails.extensionDataSourcePath`, d_up.`connectionDetails.server`, h.UpStreamId ) as UpStreamName,
    coalesce( a_up.Type, d_up.DataSourceType, h.UpStreamType ) as UpStreamType,
    coalesce(w_up.WorkspaceName, "Data Source") as UpStreamWorkspaceName,
    h.DownStreamId,
    h.DownStreamWorkspaceId,
    coalesce( a_down.name,  r_app.Name, d_down.`connectionDetails.path`,  d_down.`connectionDetails.extensionDataSourcePath`, d_down.`connectionDetails.server`, h.DownStreamId) as DownStreamName, -- r_app.Name for report names...
    coalesce( a_down.Type, d_down.DataSourceType, h.DownStreamType ) as DownStreamType,
    coalesce(w_down.WorkspaceName, "Data Source") as DownStreamWorkspaceName,
    h.SourceDF
    from hierarchy_test as h
    left join workspaces as w_down on w_down.WorkspaceId = h.DownStreamWorkspaceId
    left join workspaces as w_up on w_up.WorkspaceId = h.UpStreamWorkspaceId
    left join active_items as a_down on a_down.id = h.DownStreamId
    left join active_items as a_up on a_up.id = h.UpStreamId
    left join ( select distinct * from datasource_instances ) as d_down on d_down.DatasourceId = h.DownStreamId
    left join ( select distinct * from datasource_instances ) as d_up on d_up.DatasourceId = h.UpStreamId
    left join reports as r_app on r_app.ReportId = h.DownStreamId
""")
hierarchy.write.option("overwriteSchema", "true").saveAsTable("hierarchy", mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Dataflow Gen2 Example
# https://app.powerbi.com/groups/4588548e-2b85-4cc6-9ac5-d219c8055dfe/dataflows-gen2/f490a356-d782-4bec-aef6-a3d21e14f2f1
# Notebook Example
# https://app.powerbi.com/groups/4588548e-2b85-4cc6-9ac5-d219c8055dfe/synapsenotebooks/655c1e4c-cdd7-4740-abb1-7924c630e581
# Semantic Model Example
# https://app.powerbi.com/groups/4588548e-2b85-4cc6-9ac5-d219c8055dfe/datasets/9d498162-9b88-48f0-955d-13adfd778fb2/details
# Report Example
# https://app.powerbi.com/groups/ca0ff5ed-edc0-40e9-ae3f-c98f2c1dcdb6/reports/9bc8e79f-9f2a-4421-8292-29dbba53a2f5/52b58097f7c6090ab27a
# Lakehouse Example
# https://app.powerbi.com/groups/ca0ff5ed-edc0-40e9-ae3f-c98f2c1dcdb6/lakehouses/63ef0455-77de-41c2-97f6-b9667706383b?experience=power-bi
# Warehouse Example
# https://app.powerbi.com/groups/ca0ff5ed-edc0-40e9-ae3f-c98f2c1dcdb6/warehouses/8c123cf0-3653-424a-a508-2095a0ae8840?experience=power-bi
# Pipeline Example
# https://app.powerbi.com/groups/ca0ff5ed-edc0-40e9-ae3f-c98f2c1dcdb6/pipelines/7f2818a7-5b93-4d23-b737-6ae9add72b75
# SQL Endpoint Example
# https://app.powerbi.com/groups/ca0ff5ed-edc0-40e9-ae3f-c98f2c1dcdb6/mirroredwarehouses/f7fef445-e7be-48d7-8beb-cbda5f0b305d
def gen_url(workspace_id, item_id, type):
    type_mapping = {
        "Report":"reports",
        "SemanticModel":"datasets",
        "Dataflow":"dataflows-gen2",
        "Lakehouse":"lakehouses",
        "Warehouse":"warehouses",
        "SQLEndpoint":"mirroredwarehouses",
        "DataPipeline":"pipelines"
    }
    try:
        url = f"https://app.powerbi.com/groups/{workspace_id.lower()}/{type_mapping[type]}/{item_id.lower()}"
    except:
        url = f"https://app.powerbi.com/groups/{workspace_id}"
    
    if type == "SemanticModel":
        url += "/details"
    
    return url

gen_url_udf = udf(gen_url)
# .withColumn("url", gen_url_udf(col("WorkspaceId"), col("Id"), col("Type")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hierarchy_items = spark.sql(
"""
    select
    distinct
    h.UpStreamId as Id,
    h.UpStreamWorkspaceId as WorkspaceId,
    coalesce( a_up.name,  d_up.`connectionDetails.path`,  d_up.`connectionDetails.extensionDataSourcePath`, d_up.`connectionDetails.server`, h.UpStreamId ) as Name,
    coalesce( a_up.Type, d_up.DataSourceType, h.UpStreamType ) as Type,
    coalesce(w_up.WorkspaceName, "Data Source") as WorkspaceName
    from hierarchy_test as h
    left join workspaces as w_down on w_down.WorkspaceId = h.DownStreamWorkspaceId
    left join workspaces as w_up on w_up.WorkspaceId = h.UpStreamWorkspaceId
    left join active_items as a_down on a_down.id = h.DownStreamId
    left join active_items as a_up on a_up.id = h.UpStreamId
    left join ( select distinct * from datasource_instances ) as d_down on d_down.DatasourceId = h.DownStreamId
    left join ( select distinct * from datasource_instances ) as d_up on d_up.DatasourceId = h.UpStreamId
    left join reports as r_app on r_app.ReportId = h.DownStreamId
    where h.UpStreamId is not null

    union

    select
    distinct
    h.DownStreamId as Id,
    h.DownStreamWorkspaceId as WorkspaceId,
    coalesce( a_down.name,  r_app.Name, d_down.`connectionDetails.path`,  d_down.`connectionDetails.extensionDataSourcePath`, d_down.`connectionDetails.server`, h.DownStreamId) as Name, -- r_app.Name for report names...
    coalesce( a_down.Type, d_down.DataSourceType, h.DownStreamType ) as Type,
    coalesce(w_down.WorkspaceName, "Data Source") as WorkspaceName
    from hierarchy_test as h
    left join workspaces as w_down on w_down.WorkspaceId = h.DownStreamWorkspaceId
    left join workspaces as w_up on w_up.WorkspaceId = h.UpStreamWorkspaceId
    left join active_items as a_down on a_down.id = h.DownStreamId
    left join active_items as a_up on a_up.id = h.UpStreamId
    left join ( select distinct * from datasource_instances ) as d_down on d_down.DatasourceId = h.DownStreamId
    left join ( select distinct * from datasource_instances ) as d_up on d_up.DatasourceId = h.UpStreamId
    left join reports as r_app on r_app.ReportId = h.DownStreamId
    where h.DownStreamId is not null
"""
)

hierarchy_items.dropDuplicates(
    subset = ["Id"]
).withColumn(
    "url", gen_url_udf(
        col("WorkspaceId"), col("Id"), col("Type")
    )
).write.option("overwriteSchema", "true").saveAsTable("hierarchy_items", mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Lineage Pathing

# CELL ********************

hierarchy_to_pd = hierarchy.withColumnRenamed("UpStreamId", "parent").withColumnRenamed("DownStreamId", "child").where(col("parent").isNotNull()).where(col("child").isNotNull()).toPandas()
G = nx.DiGraph()
G.add_edges_from(hierarchy_to_pd[["parent", "child"]].values)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_path_length = 0
all_paths_dict = []
for source in G.nodes():
    for target in G.nodes():
        if source != target:
            try:
                paths = list(nx.shortest_path(G, source, target))
                if len(paths) > 0:
                    all_paths_dict.append([source, target, paths, len(paths) - 1])
            except:
                pass
        elif source == target:
            all_paths_dict.append([source, target, [source, target], 0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hierarchy_bridge = spark.createDataFrame(all_paths_dict, schema = ["source", "target", "path", "length"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hierarchy_bridge.write.option("overwriteSchema", "true").saveAsTable("hierarchy_bridge", mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
