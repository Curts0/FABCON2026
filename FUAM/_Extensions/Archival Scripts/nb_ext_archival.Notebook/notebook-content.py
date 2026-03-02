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

# ### Archival Examples
# 
# - Use Semantic Link Labs to easily export and archive Fabric items into your FUAM lakehouse.

# CELL ********************

# Retrieve BIM Files
# https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_bim

# Export Report
# https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.export_report

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

from notebookutils import mssparkutils
from sempy_labs.tom import connect_semantic_model
from sempy_labs.report import download_report

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Semantic Model - Archive Example

# CELL ********************

export_location = "Files/archive"
mssparkutils.fs.mkdirs(export_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

semantic_models = spark.sql(
"""
    SELECT 
        workspaces.WorkspaceId AS workspace_id,
        workspaces.WorkspaceName AS workspace_name,
        item.id AS semantic_model_id,
        item.name AS semantic_model_name
    FROM active_items AS item 
        LEFT JOIN workspaces on workspaces.WorkspaceId = item.workspaceId
    WHERE item.type = 'SemanticModel'
    AND workspaces.WorkspaceName = 'FUAM'
    AND item.fuam_deleted = "false"
    ORDER BY workspaces.WorkspaceName DESC
"""
)
semantic_models.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def archive_model(row):

    with connect_semantic_model(
        dataset=row.semantic_model_id, workspace=row.workspace_id, readonly = False
    ) as model:
        bim = model.get_bim()
        print("\t - bim retrieved.")


    save_location = f"{export_location}/semantic_models/{row.workspace_name}"
    mssparkutils.fs.mkdirs(save_location)
    mssparkutils.fs.put(f"{save_location}/{row.semantic_model_name}.bim", json.dumps(bim), overwrite=True)
    print("\t - archive successful!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in semantic_models.collect():
    print(f"Executing archival for {row.semantic_model_name} in {row.workspace_name}")
    # try:
    archive_model(row)
    # except:
    #     print("\t - archive skipped...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Report Archive Example

# CELL ********************

reports = spark.sql(
    """
        SELECT 
            workspaces.WorkspaceId AS workspace_id,
            workspaces.WorkspaceName AS workspace_name,
            item.id AS report_id,
            item.name AS report_name
        FROM active_items AS item 
            LEFT JOIN workspaces on workspaces.WorkspaceId = item.workspaceId
        WHERE item.type = 'Report'
        AND workspaces.WorkspaceName = 'FUAM'
        AND item.fuam_deleted = 'false'
        ORDER BY workspaces.WorkspaceName DESC
    """
)
reports.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in reports.collect():

    print(f"Executing archival for {row.report_name} in {row.workspace_name}")

    try:
        mssparkutils.fs.mkdirs(f"Files/archive/reports/{row.workspace_name}")

        download_report(
            report = row.report_name,
            file_name = f"archive/reports/{row.workspace_name}/{row.report_name}",
            download_type = "LiveConnect",
            workspace = row.workspace_name
        )

        print("\t - archived!")
    except:
        print("\t - archive skipped...")

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
