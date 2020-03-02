# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# STEP 2:Define a start date
#In this case yesterday
yesterday = datetime(2020, 2, 29)

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': models.Variable.get('project_id')
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
    'complex_workflow',
    description='DAG for deployment a Dataproc Cluster',
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args) as dag:
    
    # STEP 5: Set Operators
    
    # BashOperator
    # A simple print date
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # dataproc_operator
    # Create small dataproc cluster 
    create_dataproc = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc',
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        num_workers=2,
        zone=models.Variable.get('dataproc_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')
    
    # BashOperator
    # Sleep function to have 1 minute to check the dataproc created 
    sleep_process = BashOperator(
        task_id='sleep_process',
        bash_command='sleep 60'
    )

    # dataproc_operator
    # Delete Cloud Dataproc cluster.
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc',
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)
    
    # STEP 6: Set DAGs dependencies
    # Each task should run after have finished the task before.
    print_date >> create_dataproc >> sleep_process >> delete_dataproc