from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator
from datetime import datetime

SQL_QUERY = """
SELECT 
  s.invoice_id,
  s.datetime,
  c.city_name,
  b.branch,
  p.product_line,
  p.product_price,
  cu.gender AS customer_gender,
  cu.type AS customer_type,
  pm.method_name AS payment_method,
  s.quantity,
  s.tax,
  s.total,
  s.cogs,
  s.margin_percentage,
  s.gross_income,
  s.rating
FROM diplom.sales s
JOIN diplom.branch b ON s.branch_id = b.branch_id
JOIN diplom.city c ON b.city_id = c.city_id
JOIN diplom.product p ON s.product_id = p.product_id
JOIN diplom.customer cu ON s.customer_id = cu.customer_id
JOIN diplom.payment_method pm ON s.payment_method_id = pm.payment_method_id;
"""

def from_pg_load():
    pg_hook = PostgresHook(postgres_conn_id='dwh_postgres')
    # Подключение к ClickHouse
    ch_client = Client(
        host='clickhouse',  
        user='default',
        password='',
        database='data_mart'
    )

    # Забираем данные из PostgreSQL
    records = pg_hook.get_records(SQL_QUERY)

    # Очистим таблицу перед загрузкой
    ch_client.execute('TRUNCATE TABLE data_mart.sales_mart')

    # Загружаем данные в ClickHouse
    insert_query = """
        INSERT INTO data_mart.sales_mart (
            invoice_id, datetime, city_name, branch, product_line, product_price,
            customer_gender, customer_type, payment_method, quantity, tax, total,
            cogs, margin_percentage, gross_income, rating
        ) VALUES
    """

    ch_client.execute(insert_query, records)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 14),
    'depends_on_past': False,
    'retries': 1,
}

with DAG('pg_to_clickhouse_sales_mart',
         schedule_interval=None,
         default_args=default_args,
         catchup=False,
         tags=['diplom']) as dag:

    etl_task = PythonOperator(
        task_id='extract_load_sales_mart',
        python_callable=from_pg_load
    )

print(f"DAG loaded: {dag.dag_id}")

trigger_pg_to_clickhouse = TriggerDagRunOperator(
    task_id="trigger_data_quality_dag",
    trigger_dag_id="data_quality_dag" 
)

etl_task >> trigger_pg_to_clickhouse
