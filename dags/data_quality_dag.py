from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator
from datetime import datetime

SQL_QUERY = """
select 'diplom.sales' "table", 'Полнота' criteria, 
	'Заполнение поля "product_id" должно быть обязательным' "rule",
	100.0 goal,
	s_product_id_fullness*1.0 fact 
from(
	select
	(1- ((select count(*) from diplom.sales where product_id is null) 
		/ (select count(*) from diplom.sales))) * 100 s_product_id_fullness
) s_product_id_fullness

union

select 'diplom.sales' "table", 'Уникальность' criteria, 
	'Поле "invoice_id" должно быть уникальным' "rule",
	100.0 goal,
	s_invoice_id_unique*1.0 fact
from(
	select(
		(select sum(count)
		from(
			select invoice_id,  count(invoice_id) from diplom.sales group by invoice_id having count(invoice_id) = 1
			) id_1_count
		)
		/ 
		(select count(*) from diplom.sales)
	) * 100 s_invoice_id_unique
) s_invoice_id_unique

union

select 'diplom.sales' "table", 'Валидность' criteria, 
	'Значение в поле "rating" не должно быть больше 10' "rule",
	100.0 goal,
	s_rating_valid*1.0 fact 
from(
	select
	(1- ((select count(*) from diplom.sales where rating > 10) 
		/ (select count(*) from diplom.sales))) * 100 s_rating_valid
) s_rating_valid

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

    table_creation = """CREATE TABLE IF NOT EXISTS data_mart.quality_checks_sales
        (
            `table` String,
            `criteria` String,
            `rule` String,
            `goal` Float32,
            `fact` Float32
        )
        ENGINE = MergeTree
        ORDER BY tuple();
    """
    ch_client.execute(table_creation)
    
    # Очистим таблицу перед загрузкой
    ch_client.execute('TRUNCATE TABLE data_mart.quality_checks_sales')

    # Загружаем данные в ClickHouse
    insert_query = """
        INSERT INTO data_mart.quality_checks_sales (
            `table`, `criteria`, `rule`, `goal`, `fact`
        ) VALUES
    """

    ch_client.execute(insert_query, records)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 14),
    'depends_on_past': False,
    'retries': 1,
}

with DAG('data_quality_dag',
         schedule_interval=None,
         default_args=default_args,
         catchup=False,
         tags=['diplom']) as dag:

    etl_task = PythonOperator(
        task_id='extract_load_data_quality',
        python_callable=from_pg_load
    )

print(f"DAG loaded: {dag.dag_id}")


