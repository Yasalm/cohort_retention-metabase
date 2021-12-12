# Cohort Challenge

# Introduction
- Challenge to build a cohort analysis Visulization and building an interface using plotly - However I decided to use METABASE as self service business intelligence tool to visulize and perform EDA, and Postgres as the database.
- I Have Created a python package that extend pandas and enable to calculate cohort_retenetion at ease and apply filters. You can view it at [pandas_cohort](https://github.com/Yasalm/pandas_cohort)
    - Basic Usage:
    ```python
     df.cohort.retention(user_col, date_column, filter_by)
     ```

# Setup 
1. Use the following command to activate env & and install packages
```console
make venv
```
2. build docker images running the following command.
```console
docker compose build
```
3. Run the DB, and metabase container's.
```console
docker compose up
```
4. Go to http://0.0.0.0:3000/setup/ to setup METABASE and connect it do postgres database with credentials found in .env file. *you can find the ip address of postgree instance by running* ``` docker inspect <container_id or container_name>```
5. you should see an empty customer table due to copy over "sql/create_tables.sql to docker-entrypoint-initdb.d/create_tables.sql" which runs sql scripts at inilizations, 
6. to puplaute customer data, and create table for retetetions and retetetions_summary, you will need to run ```python 
                                                                                                          python retentions.py
                                                                                                           ```
7. to reflect changes on METABASE you will need to go to setting ->  admin page -> databases -> click "Sync database schema now" and "Re-scan field values now"
8. METABASE should reflect and update to changes.
