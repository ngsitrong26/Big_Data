import sys
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Thêm đường dẫn đến thư mục chứa các module của bạn
sys.path.append('F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda')

# Nhập các hàm cần thiết
from Batch_layer.batch_layer_task import batch_layer
from web_crawler import start_crawling

# Danh sách các URL cần thu thập
urls = ['https://vietjetair.com']

# Định nghĩa task cho Real-Time Layer
def real_time_crawl_task():
    for url in urls:
        start_crawling(url)
        
# Định nghĩa task cho Batch Layer
def historical_crawler(url):
    start_crawling(url, batch_mode=True)

# Khởi tạo DAG
with DAG(
    dag_id="daily_data_sync",
    start_date=datetime.datetime(2024, 12, 22),
    schedule_interval=None,  # Không đặt lịch cho toàn bộ DAG
    catchup=False  # Không chạy lại các task bị bỏ qua từ trước
) as dag:
    # Định nghĩa task cho Batch Layer với lịch chạy hàng ngày
    batch_layer_task = PythonOperator(
        task_id="batch_layer",
        python_callable=batch_layer,  # Chỉ định hàm callable
        schedule_interval="0 0 * * *"  # Chạy hàng ngày lúc 00:00
    )

    # Định nghĩa task cho Real-Time Layer với lịch chạy mỗi phút
    real_time_layer_task = PythonOperator(
        task_id="real_time_layer",
        python_callable=real_time_crawl_task,  # Chỉ định hàm callable
        schedule_interval="*/1 * * * *"  # Chạy mỗi phút
    )

    # Đặt thứ tự thực thi các task
    batch_layer_task >> real_time_layer_task
