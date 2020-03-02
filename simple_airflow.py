# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

# STEP 2:Define a start date
#In this case yesterday
yesterday = datetime(2020, 2, 28)

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
    'simple_workflow',
    description='Hello World =)',
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args) as dag:
    
    # STEP 5: Set Operators
    # BashOperator
    # Every operator has at least a task_id and the other parameters are particular for each one, in this case, is a simple BashOperatator this operator will execute a simple echo "Hello World!"
    helloOp = BashOperator(
        task_id='hello_world',
        bash_command='echo "Hello Worl!"'
    )
    
    # STEP 6: Set DAGs dependencies
    # Since we have only one, we just write the operator
    helloOp