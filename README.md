# End-to-End E-Commerce Data Pipeline & Customer Segmentation

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.2-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-Cloud%20DWH-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-K--Means-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

## 1. Tổng quan dự án (Project Overview)

Dự án xây dựng một hệ thống **ELT Data Pipeline khép kín (End-to-End)** xử lý dữ liệu thương mại điện tử **Olist Brazilian E-Commerce** (~100.000 đơn hàng), kết hợp mô hình hóa kho dữ liệu chuẩn **Star Schema**, phân cụm khách hàng bằng **Machine Learning (RFM + K-Means)** và trực quan hóa báo cáo quản trị trên **Power BI**.

### Quá trình nâng cấp kiến trúc (System Evolution)
* **Phiên bản 1.0 (Local MVP):** Xử lý ETL cục bộ bằng Python scripts, lưu trữ trên **SQL Server** và kết nối Power BI (`Project Dashboard.pbix`).
* **Phiên bản 2.0 (Modern Data Stack trên Cloud):** Chuyển đổi sang kiến trúc **ELT hiện đại** — đóng gói môi trường bằng **Docker Compose**, điều phối tự động 10 tác vụ bằng **Apache Airflow**, đẩy toàn bộ khối lượng tính toán biến đổi dữ liệu sang **Google BigQuery** và xuất báo cáo trực tiếp từ Cloud (`Olist_BigQuery_Dashboard.pbix`).

---

## 2. Kiến trúc hệ thống (System Architecture)

Luồng dữ liệu được điều phối tự động mỗi ngày (`@daily`) thông qua DAG **`olist_to_bigquery_pipeline`** gồm 3 pha xử lý chính:

```mermaid
flowchart LR
    subgraph Source["1. Raw Data"]
        CSV[("Olist CSV Datasets\n(Orders, Items, Customers, Products)")]
    end

    subgraph Airflow["2. Docker & Apache Airflow"]
        EL["Phase 1: Extract & Load\n(Python + Pandas + PyArrow)"]
        ELT["Phase 2: Cloud ELT\n(GoogleSQL Orchestration)"]
        ML["Phase 3: ML Pipeline\n(StandardScaler + K-Means)"]
    end

    subgraph BigQuery["3. Google BigQuery (olist_dwh)"]
        STG[("Staging Layer\nstg_orders, stg_order_items,\nstg_customers, stg_products")]
        DWH[("Star Schema & RFM\nfact_order_items, dim_customers,\ndim_products, dim_date, rfm_model")]
        OUT[("ML Output\nrfm_clusters")]
    end

    subgraph BI["4. BI & Analytics"]
        PBI["Power BI Desktop\n(Olist_BigQuery_Dashboard.pbix)"]
    end

    CSV --> EL --> STG --> ELT --> DWH --> ML --> OUT
    DWH --> PBI
    OUT --> PBI
