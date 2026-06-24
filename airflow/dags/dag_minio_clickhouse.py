from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# DAG Conf
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisialisasi DAG
dag = DAG(
    'viscon_minio_to_clickhouse',
    default_args=default_args,
    description='Extract Viscon from MinIO and Load to ClickHouse with zero local storage overhead',
    schedule_interval='30 7 * * *', # 07:30 AM or 8.30sg
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['viscon', 'clickhouse', 'minio'],
)

def _process_viscon_data(ds, **kwargs):
    """
    This function performs extraction and loading.
    All imports (pandas, hooks) are placed inside the function to avoid overloading
    the Airflow Scheduler during the DAG parsing process.
    """
    import pandas as pd
    import clickhouse_connect
    from airflow.hooks.base import BaseHook
    
    # 1. credential MinIO from Airflow UI
    minio_conn = BaseHook.get_connection('minio_conn_id') 
    
    # Airflow S3 connection biasanya menyimpan akses/secret di login/password
    print("minio_conn",minio_conn)
    print("minio_conn.host",minio_conn.host)
    print("minio_conn.port",minio_conn.port)
    print("minio_conn.login",minio_conn.login)
    print("minio_conn.password",minio_conn.password)
    print("minio_conn.extra_dejson",minio_conn.extra_dejson)
    minio_access = minio_conn.login
    minio_secret = minio_conn.password
    # minio_endpoint = minio_conn.host if minio_conn.host.startswith('http') else f"http://{minio_conn.host}"
    # minio_endpoint = minio_conn.extra_dejson.get("endpoint_url")
    endpoint_url = minio_conn.extra_dejson.get("endpoint_url")

    # Tambahkan protocol jika belum ada
    if not endpoint_url.startswith(("http://", "https://")):
        endpoint_url = f"http://{endpoint_url}"
    
    # Format tanggal berdasarkan execution_date (ds) Airflow. Misal: 20260120
    date_str = ds.replace("-", "") 
    # s3_path = f"s3://transplant-data-backup/final_plant_result/{date_str}-Good-transplant-data.csv"
    s3_path = f"s3://transplant-data-backup/final_plant_result/20260618-Good-transplant-data.csv"
    
    print(f"Membaca data langsung dari memori: {s3_path}")
    
    # 2. Baca file langsung dari jaringan ke Pandas (Tanpa save ke disk lokal)
    df = pd.read_csv(
        s3_path,
        storage_options={
            "key": minio_access,
            "secret": minio_secret,
            "client_kwargs": {"endpoint_url": f"{endpoint_url}"}
        }
    )
    
    # 3. Optimasi Memori DataFrame
    # Mengubah object ke category untuk efisiensi RAM di Docker
    df['Grade'] = df['Grade'].astype('category')
    df['Characteristic'] = df['Characteristic'].astype('category')
    df['Batch'] = df['Batch'].astype('category')
    df['MeasureTime'] = pd.to_datetime(df['MeasureTime'])
    
    # 4. Ambil kredensial ClickHouse dari Airflow UI (Tipe HTTP)
    ch_conn = BaseHook.get_connection('clickhouse_http_conn_id')
    
    print(f"Membuka koneksi ke ClickHouse di {ch_conn.host}:{ch_conn.port}")
    client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port,
        username=ch_conn.login,
        password=ch_conn.password
    )
    
    # 5. Insert data ke ClickHouse
    client.insert_df('viscon_raw', df)
    
    print(f"Sukses! {len(df)} baris data masuk ke ClickHouse.")
    
    # 6. Bersihkan memori secara eksplisit (Penting untuk container dengan RAM terbatas)
    del df
    client.close()

# Mendefinisikan Task
extract_load_task = PythonOperator(
    task_id='extract_and_load_viscon_task',
    python_callable=_process_viscon_data,
    dag=dag,
)

extract_load_task