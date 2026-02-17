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
# META       "default_lakehouse_workspace_id": "248ec193-1176-49d4-ba36-ced311d1e3c7"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Lakehouse Optimization
# This notebook uses optimize and vacuum for the lakehouse tables in order to delete unnecessary historic files and optimize performance

# PARAMETERS CELL ********************

number_of_days = 7 # must be bigger than 7 days, otherwise setting must be changed, this is used for vaccuum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC SET spark.sql.parquet.vorder.enabled

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for tab in tables:
    print(tab.name)

    print("Run Optimize")
    spark.sql("""
        OPTIMIZE FUAM_Lakehouse.""" + tab.name + """ VORDER;
    """)

    print("Run Vacuum")
    spark.sql("""
        VACUUM FUAM_Lakehouse.""" + tab.name + """ RETAIN """ + str(number_of_days * 24) + """ HOURS
    """)

    


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
