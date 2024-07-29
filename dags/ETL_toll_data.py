from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import timedelta

# Define DAG arguments
default_args = {
    'owner': 'alex_martishin',
    'start_date': days_ago(0),
    'email': ['asmartishin@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
    catchup=False,
)

# Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# Create a task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# Create a task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv >  /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# Create a task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c10-19,20-29 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

# Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste /home/project/airflow/dags/finalassignment/csv_data.csv \
          /home/project/airflow/dags/finalassignment/tsv_data.csv \
          /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
          > /home/project/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag,
)
# Transform the data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F"," \'{print toupper($4),$1,$2,$3,$5,$6,$7,$8,$9}\' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag=dag,
)

# Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

# Submit the DAG


# Unpause and trigger the DAG


# List the DAG tasks


# Monitor the DAG
