import pandas as pd
from sqlalchemy import create_engine

def load_data_to_postgres():
    
    DATABASE_URL = "postgresql://root:admin@postgres_container_2:5432/real_estate_db"
    engine = create_engine(DATABASE_URL)

    
    path = "/opt/airflow/data/clean_data.csv"
    df = pd.read_csv(path)

    # 3. Đẩy dữ liệu vào Postgres
    df.to_sql(
        name="properties",
        con=engine,
        schema='public',
        if_exists="append",
        index=False
    )
    print("Loaded data to postgres successfully")