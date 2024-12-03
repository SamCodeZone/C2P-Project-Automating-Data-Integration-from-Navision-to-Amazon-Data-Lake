# C2P-Project-Automating-Data-Integration-from-Navision-to-Amazon-Data-Lake
The C2P project automates the extraction, transformation, and ingestion of data from a Navision SQL database into Amazon's data lake in Parquet format. This pipeline leverages Python scripts and SSIS packages, enabling efficient data storage, processing, and scalability for analytics and reporting.
Key Features:
Automated Data Extraction: Retrieves structured data from Navision using timestamp-based incremental loads.
Parquet Conversion: Transforms data into Parquet format for efficient columnar storage and processing.
Amazon S3 Integration: Uploads data securely to Amazon's data lake with pre-signed URLs via HTTPS.
Dynamic Schema Mapping: Automatically maps SQL schema to Parquet-compatible types.
Robust Data Handling: Resolves issues with special characters and incompatible data types (e.g., pictures).
Project Components
Python Scripts:
Handles data extraction, transformation, and ingestion.
Updates timestamp and logs job information.
Configuration Files:
Separate JSON files for SQL connections and data lake credentials.
SSIS Packages:
Facilitates execution of Python scripts and manages data ingestion workflows.
Monitoring Scripts:
Tracks job statuses and verifies successful data ingestion.
Benefits
Improved Performance: Columnar storage in Parquet format enhances processing speed and data compression.
Enhanced Security: Separate configuration files ensure better credential management.
Scalable Solution: Seamless integration with Amazon's data lake for future expansion.
Challenges & Solutions
File Format Selection: Migrated from CSV to Parquet to handle special characters and improve data integrity.
Credential Management: Separated configuration into two JSON files for SQL and S3 settings, enhancing clarity and security.
Usage
Configure JSON files for SQL and S3 settings.
Execute createflow.py to define the flow schema.
Run Ingestscript.py via Python or SSIS, ensuring timestamp variables are set correctly.
Monitor ingestion jobs and verify status using the ingestion checker script.
Technologies Used
Database: Navision
Programming: Python (pandas, pyodbc, pyarrow)
ETL: SSIS
Cloud Storage: Amazon S3
This repository ensures seamless and efficient data integration for advanced analytics and reporting needs.
