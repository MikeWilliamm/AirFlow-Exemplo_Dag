#>pip install airflow-dag
#>pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator #Operador = o que a tarefa vai exeutar (BashOperator, PythonOperator, BranchPythonOperator, SQLOperator, EmailOperator)
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.email import send_email
import pandas as pd
import requests
import json
import random

def fun_captura_conta_dados():
    qtde = random.randint(0,100)
    return qtde

def fun_e_valido(ti):
    qtde = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if qtde > 50:
        return 'valido'
    else:
        return 'n_valido'
#Parametros: 
#1 id unico da dag
#2 start_date =  data de inicio execução dag, a partir de que data vai começar a executar, no exemplo, a partir de 2023,5,2, a meia noite, mais 30 minutos do schedule_interval que se iniciara a execução, ou seja inico em 2023,5,2 as 00h30m 
#3 schedule_interval = intervalo de execução padrão crontab linux
#4 catchup=false sempre! = Se catchup=True, a partir do start_date, até hoje, todos os dags que não fora executados o airflow vai criar uma dag e executar, assim ficando varios dags em execução
with DAG('tutorial_dag_airflow', start_date=datetime(2023,5,3),
         schedule_interval= '30 * * * *', catchup= False) as dag:
    
    #Criação das tasks
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados', #Por convenção o mesmo nome da task
        python_callable = fun_captura_conta_dados #Função
    )
    
    
    e_valido =BranchPythonOperator(
        task_id = 'e_valido',
        python_callable = fun_e_valido
    )
    
    
    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )
    
    
    n_valido = BashOperator(
        task_id = 'n_valido',
        bash_command = "echo 'Quantidade não OK'"
    )
    
    captura_conta_dados >> e_valido >> [valido, n_valido]