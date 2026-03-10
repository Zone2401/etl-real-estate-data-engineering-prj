# Real Estate ETL Pipeline

This project implements a full ETL pipeline for real estate data using Apache Airflow, Data build tool (Astronomer Cosmos). 
The pipeline scrapes data from the batdongsan.com website, processes it by using python and dbt, and loads it into Postgresql

## Architecture
<img width="914" height="653" alt="{99E56B75-68B7-4EAB-8A0B-094D0422F9FB}" src="https://github.com/user-attachments/assets/df064ba0-94b6-489b-a1bf-a2e8a10ac0fe" />



## Project Structure
<img width="682" height="389" alt="{DE7E8092-9DC3-497B-80AD-5E75B379F22E}" src="https://github.com/user-attachments/assets/5d77b33d-e9ea-4605-a124-144d23b67eff" />


## Technologies Used

- Orchestration: Apache Airflow
- Crawling: Scrapy (Python)
- Transformation: dbt (Data Build Tool)
- Database: PostgreSQL
- Infrastructure: Docker & Docker Compose

## Data Model 

<img width="989" height="709" alt="{29781FD6-EBB3-4E51-A844-621C9F7970AB}" src="https://github.com/user-attachments/assets/ca175cd9-d03d-4638-b893-6eef7a9557d7" />
