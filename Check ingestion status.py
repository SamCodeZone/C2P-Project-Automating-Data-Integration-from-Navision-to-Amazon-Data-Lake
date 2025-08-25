import requests
import pyodbc
import time
from datetime import datetime, timedelta
import json
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration for the API and database
API_BASE_URL = "url"
API_KEY = "Key"
cert_path = r"path"

# Database connection details (same as in the ingestion script)
update_config_path = Path('E:/DataLake_Project/dev/Configeration/sqlconnection.json')
with open(update_config_path, 'r', encoding='utf-8') as f:
    config = json.load(f)
update_config = config['updateconnection']

# Establish connection to the update SQL Server (using SQL Server Authentication)
update_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={update_config['update_server']};DATABASE={update_config['update_database']};UID={update_config['username']};PWD={update_config['password']};"
update_connection = pyodbc.connect(update_conn_str)

def fetch_job_status(job_id):
    """Fetch the status of a job using the job ID."""
    try:
        response = requests.get(
            url=f"{API_BASE_URL}/jobs/{job_id}/status",
            headers={"x-api-key": API_KEY},
            verify=cert_path
        )
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Failed to fetch job status: {response.status_code} - {response.text}")
    except Exception as e:
        logging.error(f"Error fetching job status for job ID {job_id}: {e}")
        sys.exit(0)
        raise

def analyze_job_status(job_status):
    """Analyze the job status response and extract relevant information."""
    try:
        job_summary = {
            'job_id': job_status[0]['job_id'],
            'flow_id': job_status[0]['flow_id'],
            'overall_status': 'InProgress',  # Default to 'InProgress'
            'total_tasks': len(job_status),
            'total_valid_rows': 0,
            'total_invalid_rows': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'tasks': []
        }

        # Check if all tasks are complete
        all_tasks_complete = True
        for task in job_status:
            task_info = {
                'task_name': task['task_name'],
                'status': task['status'],
                'start_time': task['task_start'],
                'end_time': task['task_end'],
                'metrics': task.get('metrics', {})
            }

            # Update overall job status based on task status
            if task['status'] == 'Failed':
                job_summary['overall_status'] = 'Failed'
            elif task['status'] == 'InProgress' or task['status'] == 'Blocked':
                all_tasks_complete = False

            # Aggregate valid and invalid rows
            if task_info['metrics']:
                job_summary['total_valid_rows'] += task_info['metrics'].get('output_valid_records', 0)
                job_summary['total_invalid_rows'] += task_info['metrics'].get('output_invalid_records', 0)

            # Determine the earliest start time and latest end time for the job
            if task_info['start_time']:
                if job_summary['start_time'] is None or task_info['start_time'] < job_summary['start_time']:
                    job_summary['start_time'] = task_info['start_time']
            if task_info['end_time']:
                if job_summary['end_time'] is None or task_info['end_time'] > job_summary['end_time']:
                    job_summary['end_time'] = task_info['end_time']

            job_summary['tasks'].append(task_info)

        # If all tasks are complete and none have failed, set overall status to 'Success'
        if all_tasks_complete and job_summary['overall_status'] != 'Failed':
            job_summary['overall_status'] = 'Success'

        # Calculate total duration if start and end times are available
        if job_summary['start_time'] and job_summary['end_time']:
            start_time = datetime.fromisoformat(job_summary['start_time'])
            end_time = datetime.fromisoformat(job_summary['end_time'])
            job_summary['duration_seconds'] = (end_time - start_time).total_seconds()

        return job_summary
    except Exception as e:
        logging.error(f"Error analyzing job status: {e}")
        raise

def insert_job_status_to_db(job_summary):
    """Insert the parsed job status into the database."""
    insert_sql_query = """
        INSERT INTO TblJobStatus (
            JobId, FlowID, OverallStatus, TotalTasks, TotalValidRows, TotalInvalidRows,
            StartTime, EndTime, DurationSeconds, TableName
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        with update_connection.cursor() as cursor:
            cursor.execute(
                insert_sql_query,
                job_summary['job_id'],
                job_summary['flow_id'],
                job_summary['overall_status'],
                job_summary['total_tasks'],
                job_summary['total_valid_rows'],
                job_summary['total_invalid_rows'],
                job_summary['start_time'],
                job_summary['end_time'],
                job_summary['duration_seconds'],
                job_summary['Table']  # Add table name to the job summary
            )
            update_connection.commit()
            logging.info(f"Job status for table {job_summary['Table']} inserted successfully.")
    except pyodbc.ProgrammingError as e:
        logging.error(f"SQL Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error inserting job status to DB: {e}")

def poll_job_status(job_id, timeout_minutes=20, interval_seconds=30, stuck_task_timeout_minutes=10):
    """
    Poll the job status until it is available, the job fails, or the timeout is reached.
    Wait until any "Formatted" task is complete and has non-null output_invalid_records and output_valid_records in its metrics.
    Ensure the job has at least 5 tasks before proceeding.
    """
    start_time = datetime.now()
    timeout = timedelta(minutes=timeout_minutes)
    stuck_task_timeout = timedelta(minutes=stuck_task_timeout_minutes)

    # Wait for a short time before the first status check
    initial_wait_minutes = 0.1
    logging.info(f"Waiting for {initial_wait_minutes} minutes before the first status check...")
    time.sleep(initial_wait_minutes * 60)  # Convert minutes to seconds

    while datetime.now() - start_time < timeout:
        try:
            job_status = fetch_job_status(job_id)
            if job_status:
                # Log the current status of all tasks
                logging.info(f"Current job status for job ID {job_id}:")
                for task in job_status:
                    logging.info(f"Task: {task['task_name']}, Status: {task['status']}, Start Time: {task['task_start']}, End Time: {task['task_end']}, Metrics: {task.get('metrics')}")

                # Ensure the job has at least 5 tasks
                if len(job_status) < 5:
                    logging.warning(f"Job has only {len(job_status)} tasks. Expected at least 5 tasks. Waiting for more tasks...")
                    time.sleep(interval_seconds)
                    continue

                # Check if any task has failed
                if any(task['status'] == 'Failed' for task in job_status):
                    logging.info("Job failed. Stopping polling.")
                    return job_status

                # Identify all "Formatted" tasks
                formatted_tasks = [task for task in job_status if 'formatted' in task['task_name'].lower()]

                # Check if any "Formatted" task is complete and has non-null output_invalid_records and output_valid_records in its metrics
                for formatted_task in formatted_tasks:
                    if formatted_task['status'] == 'Success':
                        metrics = formatted_task.get('metrics', {})
                        if metrics.get('output_invalid_records') is not None and metrics.get('output_valid_records') is not None:logging.info(f"Formatted task '{formatted_task['task_name']}' is complete and has non-null output_invalid_records and output_valid_records. Stopping polling.")
                        return job_status

                # Check if all non-"Formatted" tasks are complete
                non_formatted_tasks = [task for task in job_status if 'formatted' not in task['task_name'].lower()]
                completed_non_formatted_tasks = [task for task in non_formatted_tasks if task['status'] in ('Success', 'Failed')]
                incomplete_non_formatted_tasks = [task for task in non_formatted_tasks if task['status'] in ('InProgress', 'Blocked')]

                # Case 1: All non-"Formatted" tasks are complete
                if len(completed_non_formatted_tasks) == len(non_formatted_tasks):
                    logging.info("All non-'Formatted' tasks completed. Stopping polling.")
                    return job_status

                # Case 2: Some non-"Formatted" tasks are in 'InProgress' or 'Blocked'
                if len(incomplete_non_formatted_tasks) > 0:
                    logging.info(f"{len(incomplete_non_formatted_tasks)} non-'Formatted' tasks are still in progress. Waiting for {interval_seconds} seconds...")

                logging.info(f"Job is still in progress. Waiting for {interval_seconds} seconds...")
            else:
                logging.info("Job status not available yet. Waiting...")
        except Exception as e:
            logging.error(f"Error fetching job status: {e}")
            sys.exit(0)

        # Wait before retrying
        time.sleep(interval_seconds)

    # If the loop exits due to timeout
    raise TimeoutError(f"Job did not complete within {timeout_minutes} minutes.")

def fetch_most_recent_jobs_from_db():
    """Fetch the most recent job IDs and table names from the [TblDatalakeLog] table."""
    query = """
        WITH LatestJobs AS (
            SELECT 
                [Table], 
                Job_id, 
                date,
                ROW_NUMBER() OVER (PARTITION BY [Table] ORDER BY date DESC) AS RowNum
            FROM TblDatalakeLog
            WHERE Job_id IS NOT NULL
        )
        SELECT [Table], Job_id
        FROM LatestJobs
        --WHERE RowNum = 1
        order by [date] desc
    """
    try:
        with update_connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return {row.Table: row.Job_id for row in rows}
    except pyodbc.ProgrammingError as e:
        logging.error(f"SQL Error: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error fetching most recent jobs: {e}")
        return {}

def process_table(Table, job_id):
    """Process a single table: fetch, analyze, and insert job status."""
    try:
        # Poll job status until it is available, fails, or times out
        job_status = poll_job_status(job_id)
        
        # Analyze job status
        job_summary = analyze_job_status(job_status)
        
        # Add table name to the job summary
        job_summary['Table'] = Table
        
        # Print summary for debugging
        logging.info(f"Job Summary for Table {Table}:")
        logging.info(json.dumps(job_summary, indent=4))

        # Log the absence of valid rows (if needed)
        if job_summary['total_valid_rows'] == 0:
            logging.info("No valid rows found. This is a valid scenario for empty tables.")
        
        # Insert job status into the database
        insert_job_status_to_db(job_summary)
    except TimeoutError as e:
        logging.error(f"Timeout Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing table {Table}: {e}")

def main():
    """Fetch the most recent job IDs from the database and process each table."""
    # Fetch the most recent job IDs and table names from [TblDatalakeLog]
    table_job_mapping = fetch_most_recent_jobs_from_db()
    
    if not table_job_mapping:
        logging.info("No job IDs found in [TblDatalakeLog].")
        return

    # Process all tables
    for Table, job_id in table_job_mapping.items():
        logging.info(f"Processing table: {Table} with job ID: {job_id}")
        process_table(Table, job_id)
        logging.info(f"Completed processing for table: {Table} \n")

if __name__ == "__main__":
    # Process all tables
    main()
