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

# CELL ********************

from pyspark.sql.functions import explode, col

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

df.select(explode(col("Workspaces")).alias("Workspace")).collect()[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(explode(col("Workspaces")).alias("Workspace")).collect()[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
