# This is a dag called my_simple_dag.py which goes into dag folder
# The very first thing you are going to do after imports is to write routines that will serve as tasks for Operators. 
# We will be using a mixture of BashOperator and PythonOperator.

import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def greet():
    print('Writing in file')
    with open('/home/airflow/test/greet.txt', 'a+', encoding='utf8') as f:
        now = dt.datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + '\n')
    return 'Greeted'
	
def respond():
    return 'Greet Responded Again'
    
# Next things I am going to do is to define default_args and create a DAG instance.

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 9, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('my_simple_dag', default_args=default_args, schedule_interval='*/10 * * * *') as dag:

  opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')

  opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet)
  
  opr_sleep = BashOperator(task_id='sleep_me',
                             bash_command='sleep 5')

  opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)

opr_hello >> opr_greet >> opr_sleep >> opr_respond
