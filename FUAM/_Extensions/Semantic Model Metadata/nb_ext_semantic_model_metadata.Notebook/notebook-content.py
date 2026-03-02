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

# ## Semantic Model Metadata Collection
# 
# This notebook will collect data from `Files/history/inventory` data build out tables for details on your semantic models.
# 
# **NOTE** "Enhance admin APIs responses with detailed metadata must be turned on in order for this data to be collected.


# CELL ********************

from pyspark.sql.functions import explode, col, concat_ws, sha2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get Latest Inventory Data Collection

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

# MARKDOWN ********************

# ### Build out Table Schemas

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
    col("SemanticModel.id").alias("SemanticModelId"),
    col("SemanticModel.name").alias("SemanticModelName"),
    col("SemanticModel")
)
base_model_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_data_source_bridge = base_model_df.select(
    "*",
    explode(col("SemanticModel.datasourceUsages")).alias("DataSourceUsage")
).select(
    col("DataSourceUsage.datasourceInstanceId").alias("DatasourceInstanceId"),
    col("SemanticModelId")
)
base_data_source_bridge.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_table_df = base_model_df.select(
    "*",
    explode(col("SemanticModel.tables")).alias("Table")
).select(
    "SemanticModelId",
    sha2(concat_ws("-", col("SemanticModelId"),col("Table.Name")),256).alias("TableId"),
    col("Table.Name").alias("TableName"),
    col("Table.isHidden").alias("IsHidden"),
    col("Table.StorageMode").alias("StorageMode"),
    "Table"
)
base_table_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_column_df = base_table_df.select(
    "*",
    explode(col("Table.columns")).alias("Column")
).select(
    "TableId",
    sha2(concat_ws("-", col("TableId"), col("Column.name")), 256).alias("ColumnId"),
    col("Column.name").alias("ColumnName"),
    col("Column.columnType").alias("ColumnType"),
    col("Column.dataType").alias("DataType"),
    col("Column.isHidden").alias("isHidden"),
    col("Column.expression").alias("Expression")
)
base_column_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_measure_df = base_table_df.select(
    "*",
    explode(col("Table.measures")).alias("Measure")
).select(
    "TableId",
    sha2(concat_ws("-", col("TableId"), col("Measure.name")), 256).alias("MeasureId"),
    col("Measure.name").alias("MeasureName"),
    col("Measure.isHidden").alias("IsHidden"),
    col("Measure.expression").alias("Expression")
)

base_measure_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_partition_df = base_table_df.select(
    "*",
    explode(col("Table.source")).alias("Partition")
).select(
    "TableId",
    sha2(concat_ws("-", col("TableId"), col("Partition.expression")), 256).alias("PartitionId"),
    col("Partition.expression").alias("Expression"),
    col("Partition.schemaName").alias("SchemaName")
)

base_partition_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Tables to Lakehouse

# CELL ********************

base_table_df = base_table_df.drop(col("Table"))
tables = {
    "semantic_model_tables": base_table_df,
    "semantic_model_columns": base_column_df,
    "semantic_model_measures": base_measure_df,
    "semantic_model_partitions": base_partition_df,
    "semantic_model_data_source_bridge": base_data_source_bridge
}
for table, df in tables.items():
    print(f"Drop and Create -> {table}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    df.write.saveAsTable(table, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
