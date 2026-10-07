import pandas as pd
import urllib
from sqlalchemy import create_engine

print("Bắt đầu đọc dữ liệu từ file CSV...")

# Dùng Pandas đọc file olist_orders_dataset.csv trong thư mục data
# Lưu toàn bộ nội dung vào một biến tên là 'df_orders'
df_orders = pd.read_csv('data/olist_orders_dataset.csv')

print("\n--- 5 DÒNG ĐẦU TIÊN CỦA DỮ LIỆU ---")
# Lệnh head() in ra 5 dòng đầu để kiểm tra xem dữ liệu có bị lỗi font hay lệch cột không
print(df_orders.head())

print("\n--- THÔNG TIN TỔNG QUAN ---")
# Lệnh info() báo cáo số lượng dòng, số lượng cột và kiểu dữ liệu của từng cột
print(df_orders.info())



# Dùng Pandas đọc file olist_order_items_dataset.csv trong thư mục data
# Lưu toàn bộ nội dung vào một biến tên là 'df_order_items'
df_order_items = pd.read_csv('data/olist_order_items_dataset.csv')

print("\n--- 5 DÒNG ĐẦU TIÊN CỦA DỮ LIỆU ---")
# Lệnh head() in ra 5 dòng đầu để kiểm tra xem dữ liệu có bị lỗi font hay lệch cột không
print(df_order_items.head())

print("\n--- THÔNG TIN TỔNG QUAN ---")
# Lệnh info() báo cáo số lượng dòng, số lượng cột và kiểu dữ liệu của từng cột
print(df_order_items.info())



# Dùng Pandas đọc file olist_customers_dataset.csv trong thư mục data
# Lưu toàn bộ nội dung vào một biến tên là 'df_customers'
df_customers = pd.read_csv('data/olist_customers_dataset.csv')

print("\n--- 5 DÒNG ĐẦU TIÊN CỦA DỮ LIỆU ---")
# Lệnh head() in ra 5 dòng đầu để kiểm tra xem dữ liệu có bị lỗi font hay lệch cột không
print(df_customers.head())

print("\n--- THÔNG TIN TỔNG QUAN ---")
# Lệnh info() báo cáo số lượng dòng, số lượng cột và kiểu dữ liệu của từng cột
print(df_customers.info())



# Dùng Pandas đọc file olist_products_dataset.csv trong thư mục data
# Lưu toàn bộ nội dung vào một biến tên là 'df_products'
df_products = pd.read_csv('data/olist_products_dataset.csv')

print("\n--- 5 DÒNG ĐẦU TIÊN CỦA DỮ LIỆU ---")
# Lệnh head() in ra 5 dòng đầu để kiểm tra xem dữ liệu có bị lỗi font hay lệch cột không
print(df_products.head())

print("\n--- THÔNG TIN TỔNG QUAN ---")
# Lệnh info() báo cáo số lượng dòng, số lượng cột và kiểu dữ liệu của từng cột
print(df_products.info())



print("\n--- BẮT ĐẦU BƯỚC TRANSFORM (LÀM SẠCH DỮ LIỆU) ---")

# 1. Lọc dữ liệu: Chúng ta chỉ quan tâm đến các đơn hàng đã giao thành công
# Chữ 'delivered' là trạng thái giao hàng thành công trong bộ dữ liệu này
df_orders = df_orders[df_orders['order_status'] == 'delivered'].copy()
print(f"Số đơn hàng giao thành công: {len(df_orders)}")

# 2. Sửa lại kiểu dữ liệu (Data Type): 
# Đổi các cột thời gian từ dạng chữ (object/string) sang đúng chuẩn ngày tháng (datetime) của máy tính
date_columns = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]

for col in date_columns:
    df_orders[col] = pd.to_datetime(df_orders[col])

# 3. Xử lý ô trống (Missing Values): 
# Xóa bỏ (drop) những dòng đơn hàng mà không có ngày giao hàng thực tế (vì nó bị lỗi hoặc thiếu sót)
df_orders = df_orders.dropna(subset=['order_delivered_customer_date'])

# 4. Sửa lại kiểu dữ liệu (Data Type): 
# Đổi các cột thời gian từ dạng chữ (object/string) sang đúng chuẩn ngày tháng (datetime) của máy tính
date_columns_2 = ['shipping_limit_date']

for col in date_columns_2:
    df_order_items[col] = pd.to_datetime(df_order_items[col])

# 5. Xử lý ô trống (Missing Values): 
# Xóa bỏ (drop) những sản phẩm mà không có tên loại sản phẩm (vì nó bị lỗi hoặc thiếu sót)
df_products = df_products.dropna(subset=['product_category_name'])

# 6. Sửa lại kiểu dữ liệu (Data Type): 
# Đổi các cột số lượng từ dạng float64 sang int64 (vì số lượng sản phẩm không thể là số thập phân)
int_columns = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
for col in int_columns:
    df_products[col] = df_products[col].astype('int64')

print("\n--- BẮT ĐẦU BƯỚC LOAD (TẢI VÀO DATABASE) ---")

# 1. Khai báo thông tin kết nối
# !!! THAY ĐỔI DÒNG NÀY thành tên Server của bạn trong SSMS
server_name = r'.\SQLEXPRESS' 
database_name = 'Project 2'

print(f"Đang kết nối tới SQL Server: {server_name}...")

# 2. Tạo chuỗi kết nối (dùng Windows Authentication nên không cần user/pass)
params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server_name};'
    f'DATABASE={database_name};'
    r'Trusted_Connection=yes;'
)

# 3. Tạo "động cơ" (engine) để Python bơm dữ liệu vào SQL Server
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 4. Thực hiện đẩy dữ liệu
print("Đang đẩy dữ liệu vào bảng 'stg_orders'...")
try:
    # Lệnh to_sql sẽ tự động tạo bảng stg_orders trong SQL Server nếu chưa có
    # if_exists='replace': Nếu bảng đã tồn tại, nó sẽ xóa bảng cũ và tạo bảng mới
    # index=False: Không đẩy cột số thứ tự mặc định của Pandas vào database
    df_orders.to_sql('stg_orders', con=engine, if_exists='replace', index=False)
    
    print("HOÀN TẤT! Dữ liệu đã nằm gọn trong SQL Server.")
except Exception as e:
    print(f"Có lỗi xảy ra trong quá trình Load: {e}")

# 5. Thực hiện đẩy dữ liệu
print("Đang đẩy dữ liệu vào bảng 'stg_order_items'...")
try:
    # Lệnh to_sql sẽ tự động tạo bảng stg_order_items trong SQL Server nếu chưa có
    # if_exists='replace': Nếu bảng đã tồn tại, nó sẽ xóa bảng cũ và tạo bảng mới
    # index=False: Không đẩy cột số thứ tự mặc định của Pandas vào database
    df_order_items.to_sql('stg_order_items', con=engine, if_exists='replace', index=False)
    
    print("HOÀN TẤT! Dữ liệu đã nằm gọn trong SQL Server.")
except Exception as e:
    print(f"Có lỗi xảy ra trong quá trình Load: {e}")

# 6. Thực hiện đẩy dữ liệu
print("Đang đẩy dữ liệu vào bảng 'stg_customers'...")
try:
    # Lệnh to_sql sẽ tự động tạo bảng stg_customers trong SQL Server nếu chưa có
    # if_exists='replace': Nếu bảng đã tồn tại, nó sẽ xóa bảng cũ và tạo bảng mới
    # index=False: Không đẩy cột số thứ tự mặc định của Pandas vào database
    df_customers.to_sql('stg_customers', con=engine, if_exists='replace', index=False)
        
    print("HOÀN TẤT! Dữ liệu đã nằm gọn trong SQL Server.")
except Exception as e:
    print(f"Có lỗi xảy ra trong quá trình Load: {e}")

# 7. Thực hiện đẩy dữ liệu
print("Đang đẩy dữ liệu vào bảng 'stg_products'...")
try:
    # Lệnh to_sql sẽ tự động tạo bảng stg_products trong SQL Server nếu chưa có
    # if_exists='replace': Nếu bảng đã tồn tại, nó sẽ xóa bảng cũ và tạo bảng mới
    # index=False: Không đẩy cột số thứ tự mặc định của Pandas vào database
    df_products.to_sql('stg_products', con=engine, if_exists='replace', index=False)
        
    print("HOÀN TẤT! Dữ liệu đã nằm gọn trong SQL Server.")
except Exception as e:
    print(f"Có lỗi xảy ra trong quá trình Load: {e}")