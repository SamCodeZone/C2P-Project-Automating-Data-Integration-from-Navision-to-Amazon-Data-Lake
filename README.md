# C2P-Project-Automating-Data-Ingestion-from-Navision-to-Amazon-Data-Lake
C2P Project
Automates the extraction, transformation, and ingestion of data from a Navision SQL database into Amazon's data lake in Parquet format. This pipeline leverages Python scripts and SSIS packages, enabling efficient data storage, processing, and scalability for analytics and reporting.

Key Features
Automated Data Extraction: Retrieves structured data from Navision using timestamp-based incremental loads.
Parquet Conversion: Transforms data into Parquet format for efficient columnar storage and processing.
Amazon S3 Integration: Uploads data securely to Amazon's data lake with pre-signed URLs via HTTPS.
Dynamic Schema Mapping: Automatically maps SQL schema to Parquet-compatible types.
Robust Data Handling: Resolves issues with special characters and incompatible data types (e.g., pictures).
Requirements
System Requirements
Operating System: Windows Server
Python Version: 3.8+
SQL Server: Installed and configured with Navision database
Python Libraries
Ensure these libraries are installed:

pandas
pyodbc
pyarrow
requests
Other Requirements
SSIS Installation: SQL Server Integration Services must be installed and configured.
Amazon S3 Access: Proper permissions and credentials to upload data to Amazon's S3.
Project Components
Python Scripts
Handles data extraction, transformation, and ingestion.
Updates timestamp and logs job information.
Configuration Files
Separate JSON files for SQL connections and data lake credentials.
SSIS Packages
Facilitates execution of Python scripts and manages data ingestion workflows.
Monitoring Scripts
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
Programming: Python (pandas, pyodbc, pyarrow, requests)
ETL: SSIS
Cloud Storage: Amazon S3

<img width="1360" height="451" alt="1" src="https://github.com/user-attachments/assets/cadaa715-057d-470d-8ff4-10a219babd20" />

<img width="1357" height="414" alt="2" src="https://github.com/user-attachments/assets/ba004f9d-c3ae-4262-9d0d-3e433a0e7157" />

<img width="1361" height="622" alt="3" src="https://github.com/user-attachments/assets/5a09814a-554e-40df-879c-441481bca873" />

<img width="1600" height="747" alt="s-blob-v1-IMAGE-AY0Fki2DnuQ" src="https://github.com/user-attachments/assets/22ae9f79-f826-47bd-979d-aee0af725621" />



