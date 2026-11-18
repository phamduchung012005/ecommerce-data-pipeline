from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Định nghĩa các hành động nhỏ (Hàm Python)
def extract_data():
    print("Đang lấy dữ liệu từ Kaggle CSV...")

def transform_data():
    print("Đang làm sạch dữ liệu và tính RFM...")

def load_data():
    print("Đang đẩy dữ liệu vào kho SQL Server...")

# 2. Khởi tạo DAG (Nhạc trưởng)
with DAG(
    dag_id='demo_luong_etl_cua_hung', # Tên luồng hiển thị trên web
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu
    schedule_interval='@daily',       # Chạy mỗi ngày (Giống Task Scheduler)
    catchup=False
) as dag:

    # 3. Giao việc cho các khối (Tasks)
    task1 = PythonOperator(task_id='Extract', python_callable=extract_data)
    task2 = PythonOperator(task_id='Transform', python_callable=transform_data)
    task3 = PythonOperator(task_id='Load', python_callable=load_data)

    # 4. Vẽ sơ đồ luồng (Phép màu của Airflow nằm ở dòng này!)
    task1 >> task2 >> task3