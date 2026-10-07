import pandas as pd
import urllib
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

print("--- KHỞI ĐỘNG MODULE MACHINE LEARNING ---")

# 1. Kết nối Data Warehouse (SQL Server)
server_name = r'.\SQLEXPRESS'
database_name = 'Project 2'

params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server_name};'
    f'DATABASE={database_name};'
    r'Trusted_Connection=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 2. Extract: Kéo bảng RFM từ SQL Server lên Python
print("Đang đọc dữ liệu RFM từ Data Warehouse...")
query = "SELECT * FROM rfm_model"
df_rfm = pd.read_sql(query, con=engine)
print(f"Đã tải {len(df_rfm)} khách hàng để phân tích.")

# 3. Transform: Chuẩn hóa dữ liệu (StandardScaler)
# K-Means dùng toán học để đo khoảng cách. 
# Cột Monetary (Tiền) lên tới hàng ngàn, trong khi Frequency (Tần suất) chỉ từ 1-5.
# Nếu không chuẩn hóa, thuật toán sẽ bị "thiên vị" bởi cột Tiền.
scaler = StandardScaler()
rfm_features = df_rfm[['recency', 'frequency', 'monetary']]
rfm_scaled = scaler.fit_transform(rfm_features)

# 4. Machine Learning: Chạy thuật toán K-Means
print("Đang chạy thuật toán K-Means phân cụm...")
# n_clusters=4: Chúng ta chia khách hàng làm 4 nhóm (VD: VIP, Trung thành, Tiềm năng, Sắp rời bỏ)
# random_state=42: Đảm bảo mỗi lần chạy đều ra kết quả nhất quán
kmeans = KMeans(n_clusters=4, random_state=42)
df_rfm['cluster'] = kmeans.fit_predict(rfm_scaled)

# 5. Load: Đẩy kết quả phân cụm ngược lại Data Warehouse
print("Đang lưu kết quả phân cụm về lại SQL Server...")
try:
    df_rfm.to_sql('rfm_clusters', con=engine, if_exists='replace', index=False)
    print("HOÀN TẤT! Đã tạo bảng 'rfm_clusters' thành công.")
except Exception as e:
    print(f"Có lỗi khi lưu kết quả: {e}")