# Formula1-Racing-Project-using-PySpark-on-Databricks
![image](https://github.com/swarupmishal/Formula1-Racing-Project-using-PySpark-on-Databricks/assets/25344771/fb72a5f8-d08f-4ad9-aa2b-574e47a1dd2b)<br>

### Project Overview:
Built a Formula 1 Data Engineering project using Spark on Azure Databricks. Formula 1 season happens once a year roughly 20 races. Each race happens over a weekend. Roughly 10 teams (constructors) participate in a season. Each team have two drivers who participate in the race. Two drivers get qualified from the entire team and they get to start the race earlier. Each driver can have multiple pit stops to change tires or fix damaged car. Based on the race results, driver standings and constructor standings are decided. The top of the drivers standings becomes the drivers' champion and the team that tops the constructor standings, becomes the constructors' champion.<br>

### Data:
Data can be downloaded from http://ergast.com/mrd/

### ER Diagram:
![image](https://github.com/swarupmishal/Formula1-Racing-Project-using-PySpark-on-Databricks/assets/25344771/8f0a9c00-0d95-4c2d-9925-fa32279e7e43)

### Data Ingestion Requirements:
- Ingest all files into data lake
- Ingested data must've schema applied
- Ingested data must've audit columns
- Ingested data must be stored in columnar format (i.e. parquet)
- Must be able to analyze data using SQL
- Ingestion logic must be able to handle incremental load

### Data Transformation Requirements:
- Join the key information required to create a new table
- Join the key information for analysis to create a new table
- Transformed tables must have audit columns
- Must be able to analyze using SQL
- Store transformed data in columnar format (i.e. parquet)
- Transformation logic must be able to handle incremental load

### Reporting Requirements:
- Get Driver Standings as shown below<br>

![image](https://github.com/swarupmishal/Formula1-Racing-Project-using-PySpark-on-Databricks/assets/25344771/35fc3ef6-805d-4424-a599-d1d9fee6ba85)

- Get Constructor Standings as shown below<br>

![image](https://github.com/swarupmishal/Formula1-Racing-Project-using-PySpark-on-Databricks/assets/25344771/321678d7-8019-42ef-94f0-a53073a3e88a)


# Solution Architecture:
I've exported data from Ergast website and imported into ADLS raw layer. This data is ingested to processed layer in the parquet format. Additional columns are also added for auditing purpose. This data is transformed using delta lake for importing into the presentation layer. Data is used for reporting and BI purpose from the presentation layer. Azure Data Factory is used to build data pipelines for scheduling our pipelines.

![image](https://github.com/swarupmishal/Formula1-Racing-Project-using-PySpark-on-Databricks/assets/25344771/74255edc-12d6-45a2-b64e-84b0d1901b59)
