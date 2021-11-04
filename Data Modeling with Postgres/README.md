# Data Modeling with Postgres


The project is modeling user activity data generated in a music streaming app (Sparkify). The objective is to build an ETL to convert the logs and song datasets into a relational database optimised for queries related to song play analysis.

## Project Structure


Data Modeling with Postgres
|____data			# Dataset
| |____log_data
| | |____...
| |____song_data
| | |____...
|
|____notebook			# notebook for developing and testing ETL
| |____etl.ipynb		    # developing ETL builder
| |____test.ipynb		    # testing ETL builder
| |____cheat-sheet.pdf
|
|____src			# source code
| |____etl.py			    # ETL builder
| |____sql_queries.py		    # ETL query helper functions
| |____create_tables.py		    # database/table creation script
