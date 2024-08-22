# Project name: Renewable Data Rangers

## Team Members
- [Tiago Reis](https://github.com/Tiago-B-C-Reis)

## Project Description
The purpose of this project is to develop a Business Intelligence (BI) solution focused on renewable energy production. The project takes raw data on global energy production over time and outputs a data product that consists of a historical data store with key performance indicators (KPIs) and information related to renewable energy. This data product will be used for BI reporting and can also support Machine Learning (ML) and Data Mining (DM) to predict the amount of renewable energy production by type, year, and country.

## Hands-on Project: Renewable Data Rangers

### Architecture Overview
Given the current focus on processing data for weekly updates, a batch processing architecture was chosen for this project. However, depending on future needs, it could evolve into a Lambda architecture to enable streaming processing.

The development of this project is divided into two phases:

### Development Architecture
The goal during this phase is to construct and test the project, including new features and tools, while maintaining a zero-cost budget. The current architecture is depicted below:

![Dev_Architecture.png](/Readme_images/dev_architecture.png)

### Production Architecture
To simulate a professional environment and improve my skills with AWS, the production version of this project will be implemented in the cloud (AWS). 

This will allow the project to:
- Access the most recent and competitive tools
- Achieve faster time-to-market (as hardware infrastructure already exists)
- Benefit from scalability and flexibility
- Save costs (no need for full on-premise investment, and payment is based on usage)
- Prevent data loss
- Enhance security
- Enable better collaboration

![Prod_Architecture.png](/Readme_images/prod_architecture.png)

### Architecture Components

#### Data Source
The project uses the API available at [https://data.un.org/](https://data.un.org/) to gather information related to renewable energy production by commodity type, covering data from the 1990s onwards across the globe. The API data is collected as an XML file and converted to CSV format by a Python script utilizing the `requests` library. This data is then delivered directly to an AWS S3 bucket.

#### Ingestion
Airbyte is used to ingest data from the S3 bucket into HDFS. This tool allows for filtering and small transformations, organizing the ingested files by stream, scheduling ingestions, and performing other actions.

#### Storage and Processing Layer
Data is organized into different layers based on quality and refinement:

- **Bronze Layer (Raw Data):** For scalability, fault tolerance, and performance (through Data Locality), Apache Hadoop HDFS is used as the Bronze Layer. In AWS EMR, this HDFS storage is ephemeral, as it only functions when the EMR clusters are active.
  
- **Silver (Curated and Aggregated Data) and Gold Layer (Cleaned and Enriched Data):**
  - Data transformation processing is handled by **Apache Spark**, chosen for:
    - **Speed:** In-memory computing and an optimized execution engine.
    - **Scalability:** Distributed processing allowing horizontal scaling across clusters, compatible with various cluster managers like Hadoop YARN, Mesos, Kubernetes, and standalone.
    - **Integration:** Works well with the Hadoop ecosystem, cloud services, and various data sources, making it highly flexible.
    - **Fault Tolerance:** Resilient Distributed Datasets (RDDs) ensure fault tolerance with mechanisms to recover lost data.
    - **Versatility:** Supports batch processing, real-time stream processing, machine learning (via MLlib), and graph processing (via GraphX).
    - **Ease of Use:** High-level APIs in Python, Java, Scala, and R, along with interactive shells, make it accessible and user-friendly.
    - **Active Ecosystem:** A strong community and a rich ecosystem of tools enhance Spark's capabilities.
  - **Intermediate storage** is managed using **PostgreSQL**, which serves as an OLTP system for the Silver and Gold Layers.
  - **Final data storage** is handled by **Amazon Redshift**, a Data Warehouse optimized for storing historical data and supporting BI activities (reports, dashboards, analytics). 
    - **Note:** Due to cost considerations, Amazon Redshift will be developed but not deployed in production.

#### Serving Layer
- **ML/DM:** Python, alongside libraries like scikit-learn, will be used to explore ML models and their potential applications.
- **Analytics:** Tableau will be used for BI, instead of PowerBI, for learning purposes.

#### Orchestration Layer
- **Apache Airflow** will be used to schedule, orchestrate, and monitor the entire pipeline:
  - **Scalability:** Capable of handling large-scale workflows across multiple machines.
  - **Monitoring:** Provides a web-based UI for real-time monitoring, troubleshooting, and managing workflows. It also automatically retries failed tasks, improving workflow reliability.
  - **Extensibility:** Supports custom plugins, operators, and integrations, allowing flexibility in connecting to various data sources and systems.
  - **Community Support:** Backed by a large open-source community, with frequent updates and a rich ecosystem of tools and plugins.

#### Security, Monitoring, and Operations Layer
- Basic tools shown in the architecture image will be used, and for the VPC, the following architecture will be implemented:

![AWS_VPC.png](/Readme_images/AWS_VPC.png)


## Development Environment Tools Repository
- **Apache Hadoop:**
  - [Docker-Hadoop-Spark](https://github.com/Marcel-Jan/docker-hadoop-spark/tree/master)
- **Apache Airbyte:**
  - [Airbyte OSS Quickstart](https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart)
- **Apache Airflow:**
  - [Airflow Docker-Compose Setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- **Apache Spark:**
  - [Spark Downloads](https://spark.apache.org/downloads.html)

## Next Steps:
- **Bronze Layer:** Redesign this storage layer, as the current Apache Hadoop HDFS storage in the production environment is temporary.
- **Development Environment:** The development environment is nearly complete, with only the ML layer remaining. The next step is to implement the production environment, including the application of the security layer.