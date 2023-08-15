from airflow import DAG
from airflow.operators.bash.BashOperator import BashOperator
from airflow.operators.python.PythonOperator import PythonOperator
from datetime import datetime, timedelta

#  Define DAG arguments

default_args = {
    'owner' : 'PRACHI',
    'start_date' : datetime.today(),
    'email' : ['dum9999@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry': True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

# Define the DAG

dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1),
)

# define the tasks

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -zxf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv ',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 tollplaza-data.tsv > tsv_data.csv ',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command =' cut -c 1-2,9-10 payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)


# task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >>extract_data_from_fixed_width >> consolidate_data >> transform_data




