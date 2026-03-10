from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_properties_tables():
    # Sử dụng connection ID thống nhất (nên tạo 're_connection' trong Airflow UI)
    hook = PostgresHook(postgres_conn_id='re_connection')
    
    sql_command = """
    DROP TABLE IF EXISTS properties;
    CREATE TABLE properties (
        title TEXT,
        price FLOAT8,
        price_unit TEXT,
        area FLOAT8,
        area_unit TEXT,
        district TEXT,
        city TEXT,
        bedroom INT,
        bathroom INT,
        news_type TEXT,
        posted_date DATE,
        expired_date DATE,
        seller TEXT,
        total_posts INT, 
        url TEXT,        
        zalo_url TEXT   
    );
    """
    
    hook.run(sql_command)
    print("Bảng 'properties' đã được tạo mới thành công!")