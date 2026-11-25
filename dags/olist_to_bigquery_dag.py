from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

PROJECT_ID = 'olist-data-warehouse-514916' 
DATASET_ID = 'olist_dwh'
CREDENTIALS_PATH = '/opt/airflow/dags/gcp_credentials.json'
DATA_DIR = '/opt/airflow/data'

def get_bq_client():
    return bigquery.Client.from_service_account_json(CREDENTIALS_PATH, project=PROJECT_ID)

def load_df_to_bigquery(df, table_name, write_disposition="WRITE_TRUNCATE"):
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Đã tải thành công {len(df)} dòng vào {table_id} (Chế độ: {write_disposition})")

def run_bq_sql(sql):
    client = get_bq_client()
    query_job = client.query(sql)
    query_job.result()
    print("Thực thi truy vấn ELT trên BigQuery thành công!")

# --- PHA 1: EXTRACT & LOAD VÀO STAGING ---
def etl_orders():
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.stg_orders"

    # 1. Truy vấn mốc thời gian mới nhất (High-Watermark) hiện có trên BigQuery
    watermark_dt = None
    try:
        client.get_table(table_id)
        query = f"SELECT MAX(order_purchase_timestamp) AS max_ts FROM `{table_id}`"
        result = list(client.query(query).result())
        last_watermark = result[0].max_ts
        if last_watermark is not None:
            watermark_dt = pd.to_datetime(last_watermark)
            # Đồng bộ bỏ múi giờ UTC để khớp với kiểu datetime đọc từ file CSV
            if watermark_dt.tzinfo is not None:
                watermark_dt = watermark_dt.tz_localize(None)
    except NotFound:
        print("Bảng stg_orders chưa tồn tại trên BigQuery. Tiến hành nạp toàn bộ lần đầu (Full Load).")

    # 2. Đọc và làm sạch dữ liệu nguồn
    df = pd.read_csv(f"{DATA_DIR}/olist_orders_dataset.csv")
    df = df[df['order_status'] == 'delivered'].copy()
    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    df = df.dropna(subset=['order_delivered_customer_date'])

    # 3. Lọc dữ liệu tăng dần (Incremental Filter)
    if watermark_dt is not None:
        print(f"High-Watermark hiện tại trên BigQuery: {watermark_dt}")
        df = df[df['order_purchase_timestamp'] > watermark_dt]

    if len(df) == 0:
        print("Không phát hiện đơn hàng mới nào sau mốc Watermark. Bỏ qua bước nạp để tối ưu chi phí!")
        return

    print(f"Phát hiện {len(df)} đơn hàng mới. Tiến hành nạp nối tiếp (WRITE_APPEND)...")
    load_df_to_bigquery(df, 'stg_orders', write_disposition="WRITE_APPEND")

def etl_order_items():
    df = pd.read_csv(f"{DATA_DIR}/olist_order_items_dataset.csv")
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
    df = df.dropna(subset=['shipping_limit_date'])
    load_df_to_bigquery(df, 'stg_order_items')

def etl_customers():
    df = pd.read_csv(f"{DATA_DIR}/olist_customers_dataset.csv")
    load_df_to_bigquery(df, 'stg_customers')

def etl_products():
    df = pd.read_csv(f"{DATA_DIR}/olist_products_dataset.csv")
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=['product_category_name'])
    int_cols = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    for col in int_cols:
        df[col] = df[col].astype('int64')
    load_df_to_bigquery(df, 'stg_products')

# --- PHA 2: TRANSFORM THÀNH STAR SCHEMA & RFM TRÊN BIGQUERY ---
def build_dim_customers():
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.dim_customers` AS
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM `{PROJECT_ID}.{DATASET_ID}.stg_customers`;
    """
    run_bq_sql(sql)

def build_dim_products():
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.dim_products` AS
    SELECT 
        product_id,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM `{PROJECT_ID}.{DATASET_ID}.stg_products`;
    """
    run_bq_sql(sql)

def build_fact_order_items():
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.fact_order_items` AS
    SELECT 
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,
        o.customer_id,
        o.order_status,
        DATE(o.order_purchase_timestamp) AS order_purchase_date,
        i.price,
        i.freight_value
    FROM `{PROJECT_ID}.{DATASET_ID}.stg_order_items` i
    JOIN `{PROJECT_ID}.{DATASET_ID}.stg_orders` o ON i.order_id = o.order_id;
    """
    run_bq_sql(sql)

def build_dim_date():
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.dim_date` AS
    SELECT DISTINCT 
        order_purchase_date AS date_key,
        EXTRACT(YEAR FROM order_purchase_date) AS year,
        EXTRACT(MONTH FROM order_purchase_date) AS month,
        EXTRACT(DAY FROM order_purchase_date) AS day,
        EXTRACT(QUARTER FROM order_purchase_date) AS quarter,
        FORMAT_DATE('%A', order_purchase_date) AS weekday
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_order_items`
    WHERE order_purchase_date IS NOT NULL;
    """
    run_bq_sql(sql)

def build_rfm_model():
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.rfm_model` AS
    WITH max_date_cte AS (
        SELECT MAX(order_purchase_date) AS max_date 
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_order_items`
    )
    SELECT 
        c.customer_unique_id,
        DATE_DIFF(m.max_date, MAX(f.order_purchase_date), DAY) AS recency,
        COUNT(DISTINCT f.order_id) AS frequency,
        SUM(f.price) AS monetary
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_order_items` f
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c ON f.customer_id = c.customer_id
    CROSS JOIN max_date_cte m
    GROUP BY c.customer_unique_id, m.max_date;
    """
    run_bq_sql(sql)

# --- PHA 2.5: TRẠM KIỂM SOÁT CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY GATE) ---
def run_data_quality_checks():
    client = get_bq_client()
    print("Bắt đầu chạy bộ kiểm thử chất lượng dữ liệu (Data Quality Checks) trên BigQuery...")

    # Kiểm tra 1: Bảng Fact không rỗng và giá bán (price) phải dương, không NULL
    q_fact = f"""
    SELECT 
        COUNT(*) AS total_rows,
        COUNTIF(price IS NULL OR price <= 0) AS invalid_price_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_order_items`
    """
    res_fact = list(client.query(q_fact).result())[0]
    if res_fact.total_rows == 0 or res_fact.invalid_price_rows > 0:
        raise ValueError(f"[DQ FAILED] fact_order_items lỗi: total={res_fact.total_rows}, invalid_price={res_fact.invalid_price_rows}")
    print(f"[PASS] fact_order_items hợp lệ ({res_fact.total_rows} dòng, 0 lỗi giá bán).")

    # Kiểm tra 2: Khóa chính customer_id trong dim_customers không bị trùng lặp
    q_dim_cust = f"""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT customer_id) AS unique_keys
    FROM `{PROJECT_ID}.{DATASET_ID}.dim_customers`
    """
    res_cust = list(client.query(q_dim_cust).result())[0]
    if res_cust.total_rows == 0 or res_cust.total_rows != res_cust.unique_keys:
        raise ValueError(f"[DQ FAILED] dim_customers trùng lặp khóa chính: total={res_cust.total_rows}, unique={res_cust.unique_keys}")
    print(f"[PASS] dim_customers toàn vẹn khóa chính ({res_cust.unique_keys} khách hàng).")

    # Kiểm tra 3: Miền giá trị toán học của bảng RFM trước khi đưa vào K-Means
    q_rfm = f"""
    SELECT 
        COUNT(*) AS total_rows,
        COUNTIF(recency IS NULL OR recency < 0 OR frequency IS NULL OR frequency < 1 OR monetary IS NULL OR monetary <= 0) AS invalid_rfm_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.rfm_model`
    """
    res_rfm = list(client.query(q_rfm).result())[0]
    if res_rfm.total_rows == 0 or res_rfm.invalid_rfm_rows > 0:
        raise ValueError(f"[DQ FAILED] rfm_model có dữ liệu bất thường: total={res_rfm.total_rows}, invalid={res_rfm.invalid_rfm_rows}")
    print(f"[PASS] rfm_model đạt chuẩn đầu vào Machine Learning ({res_rfm.total_rows} bản ghi).")

# --- PHA 3: MACHINE LEARNING PHÂN CỤM KHÁCH HÀNG ---
def run_kmeans_clustering():
    client = get_bq_client()
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.rfm_model`"
    df_rfm = client.query(query).to_dataframe()
    print(f"Đã tải {len(df_rfm)} khách hàng từ BigQuery để phân cụm.")

    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(df_rfm[['recency', 'frequency', 'monetary']])

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df_rfm['cluster'] = kmeans.fit_predict(rfm_scaled)

    load_df_to_bigquery(df_rfm, 'rfm_clusters')

# --- KHAI BÁO DAG & LUỒNG ĐIỀU PHỐI ---
with DAG(
    dag_id='olist_to_bigquery_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=2,
    tags=['olist', 'bigquery', 'elt']
) as dag:

    # Tasks Staging
    t_orders = PythonOperator(task_id='etl_stg_orders', python_callable=etl_orders)
    t_items = PythonOperator(task_id='etl_stg_order_items', python_callable=etl_order_items)
    t_customers = PythonOperator(task_id='etl_stg_customers', python_callable=etl_customers)
    t_products = PythonOperator(task_id='etl_stg_products', python_callable=etl_products)

    # Tasks Data Warehouse & RFM
    t_dim_customers = PythonOperator(task_id='build_dim_customers', python_callable=build_dim_customers)
    t_dim_products = PythonOperator(task_id='build_dim_products', python_callable=build_dim_products)
    t_fact_items = PythonOperator(task_id='build_fact_order_items', python_callable=build_fact_order_items)
    t_dim_date = PythonOperator(task_id='build_dim_date', python_callable=build_dim_date)
    t_rfm = PythonOperator(task_id='build_rfm_model', python_callable=build_rfm_model)

    # Task Data Quality Gate & Machine Learning
    t_dq = PythonOperator(task_id='data_quality_checks', python_callable=run_data_quality_checks)
    t_kmeans = PythonOperator(task_id='run_kmeans_clustering', python_callable=run_kmeans_clustering)

    # Thiết lập luồng phụ thuộc đa nhánh
    t_customers >> t_dim_customers
    t_products >> t_dim_products
    [t_orders, t_items] >> t_fact_items
    t_fact_items >> t_dim_date
    [t_fact_items, t_dim_customers] >> t_rfm

    # Toàn bộ Star Schema & RFM phải vượt qua trạm kiểm soát DQ trước khi huấn luyện K-Means
    [t_dim_products, t_dim_date, t_rfm] >> t_dq >> t_kmeans