from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import requests
import io

default_args = {
    'start_date': datetime(2024, 1, 1),
}

URL = 'https://u.netology.ru/backend/uploads/lms/content_assets/file/11596/sales.csv?_gl=1*1mt58ue*_gcl_au*MjQ2NDU2NTk1LjE3NDc1MDYxMDc.'

def download_csv(**context):
    response = requests.get(URL)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))
    context['ti'].xcom_push(key='raw_data', value=df.to_json(orient='records'))

def transform_and_load(**context):
    raw_data = context['ti'].xcom_pull(task_ids='download_csv', key='raw_data')
    df = pd.read_json(raw_data, orient='records')
    pg_hook = PostgresHook(postgres_conn_id='dwh_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    #Справочники
    city_id_map = {}
    branch_id_map = {}
    customer_id_map = {}
    product_id_map = {}
    payment_id_map = {}
    #City
    cities = df['City'].unique()
    for city in cities:
        cursor.execute("""
            INSERT INTO diplom.city (city_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            RETURNING city_id
        """, (city,))
        result = cursor.fetchone()
        if result is not None:
            city_id = result[0]
        else:
            cursor.execute("SELECT city_id FROM diplom.city WHERE city_name = %s", (city,))
            city_id = cursor.fetchone()[0]
        city_id_map[city] = city_id

    #Branch
    branches = df[['Branch', 'City']].drop_duplicates()
    for _, row in branches.iterrows():
        branch_name = row['Branch']
        city_name = row['City']
        city_id = city_id_map[city_name]

        cursor.execute("""
            INSERT INTO diplom.branch (branch, city_id)
            VALUES (%s, %s)
            ON CONFLICT (branch, city_id) DO NOTHING
            RETURNING branch_id
        """, (branch_name, city_id))
        result = cursor.fetchone()
        if result is not None:
            branch_id = result[0]
        else:
            cursor.execute("""
                SELECT branch_id FROM diplom.branch
                WHERE branch = %s AND city_id = %s
            """, (branch_name, city_id))
            branch_id = cursor.fetchone()[0]
        
        branch_id_map[(branch_name, city_name)] = branch_id
    
    #Customer -- условно уникальным покупателем будем считать связку Customer_type, Gender, т.к. нет другой информации о покупателях

    customers = df[['Customer type', 'Gender']].drop_duplicates()
    for _, row in customers.iterrows():
        customer_type = row['Customer type']
        customer_gender = row['Gender']

        cursor.execute("""
            INSERT INTO diplom.customer (gender, type)
            VALUES (%s, %s)
            ON CONFLICT (gender, type) DO NOTHING
            RETURNING customer_id
        """, (customer_gender, customer_type))
        result = cursor.fetchone()
        if result is not None:
            customer_id = result[0]
        else:
            cursor.execute("""
                SELECT customer_id FROM diplom.customer
                WHERE gender = %s AND type = %s
            """, (customer_gender, customer_type))
            customer_id = cursor.fetchone()[0]
        
        customer_id_map[(customer_gender, customer_type)] = customer_id   

    #Product
    
    products = df[['Product line', 'Unit price']].drop_duplicates()
    for _, row in products.iterrows():
        product_line = row['Product line']
        unit_price = row['Unit price']
        cursor.execute("""
            INSERT INTO diplom.product (product_line, product_price)
            VALUES (%s, %s)
            ON CONFLICT (product_line, product_price) DO NOTHING
            RETURNING product_id
        """, (product_line, unit_price))
        result = cursor.fetchone()
        if result is not None:
            product_id = result[0]
        else:
            cursor.execute("""
                SELECT product_id FROM diplom.product
                WHERE product_line = %s AND product_price = %s
            """, (product_line, unit_price))
            product_id = cursor.fetchone()[0]
        
        product_id_map[(product_line, unit_price)] = product_id

    #Payment_method
    
    payments = df['Payment'].unique()
    for payment in payments:
        cursor.execute("""
            INSERT INTO diplom.payment_method (method_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            RETURNING payment_method_id
        """, (payment,))
        result = cursor.fetchone()
        if result is not None:
            payment_id = result[0]
        else:
            cursor.execute("SELECT payment_method_id FROM diplom.payment_method WHERE method_name = %s", (payment,))
            payment_id = cursor.fetchone()[0]
        payment_id_map[payment] = payment_id

    #Sales
    df['datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))


    for _, row in df.iterrows():
        sales_id = row['Invoice ID']
        branch_id_ = branch_id_map.get((row['Branch'], row['City']))
        customer_id_ = customer_id_map.get((row['Gender'], row['Customer type']))
        product_id_ = product_id_map.get((row['Product line'], row['Unit price']))
        payment_id_ = payment_id_map.get(row['Payment'])
        quantity = row['Quantity']
        tax = row['Tax 5%']
        total_ = row['Total']
        cogs = row['cogs']
        margin = row['gross margin percentage']
        income = row['gross income']
        rating = row['Rating']
        datetime_ = row['datetime']

        cursor.execute("""
            INSERT INTO diplom.sales (invoice_id, product_id, customer_id, 
                       branch_id, payment_method_id, datetime, quantity,
                       tax, total, cogs, margin_percentage, gross_income, rating
                       )
            VALUES (%s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s)
        """, (sales_id, product_id_, customer_id_, branch_id_, 
              payment_id_, datetime_, quantity, tax, total_, cogs, margin, income, rating))

    conn.commit()
    cursor.close()
    conn.close()

with DAG('from_csv_to_pg',
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=['diplom'],
         description='ETL для проекта диплома') as dag:

    download = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv
    )

    transform_load = PythonOperator(
        task_id='normalize_and_insert',
        python_callable=transform_and_load
    )

    download >> transform_load


trigger_pg_to_clickhouse = TriggerDagRunOperator(
    task_id="trigger_pg_to_clickhouse_sales_mart",
    trigger_dag_id="pg_to_clickhouse_sales_mart" 
)

transform_load >> trigger_pg_to_clickhouse  

