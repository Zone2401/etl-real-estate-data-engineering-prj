import pandas as pd
import numpy as np
from pathlib import Path


DATA_DIR = Path("/opt/airflow/data")
RAW_FILE = DATA_DIR / "raw_data.json"
CLEAN_FILE = DATA_DIR / "clean_data.csv"


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.replace(["None", "Giá thỏa thuận", ""], np.nan, inplace=True)
    df.drop_duplicates(inplace=True)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # price
    df["price"] = df["price"].str.replace(",", ".", regex=False)
    price_df = df["price"].str.extract(
        r'(?P<price>\d+\.?\d*)\s*(?P<price_unit>.*)'
    )
    df["price"] = price_df["price"]
    df["price_unit"] = price_df["price_unit"]

    # area
    area_df = df["area"].str.extract(
        r'(?P<area>\d+\.?\d*)\s*(?P<area_unit>.*)'
    )
    df["area"] = area_df["area"]
    df["area_unit"] = area_df["area_unit"]

    # address
    address_split = df["address"].str.split(",", n=1, expand=True)
    df["district"] = address_split[0].str.strip()
    df["city"] = address_split[1].str.strip()

    # bedroom
    df["bedroom"] = (
        df["bedroom"]
        .str.replace("Phòng ngủ", "", regex=False)
        .astype(float)
    )

    # bathroom
    df["bathroom"] = (
        df["bathroom"]
        .str.replace("WC", "", regex=False)
        .astype(float)
    )

    # news_type
    df["news_type"] = df["news_type"].str.replace("Tin", "", regex=False)

    # total_posts
    df["total_posts"] = (
        df["total_posts"]
        .str.extract(r"(\d+)")
        .astype(float)
    )

    df.drop(columns=["address"], inplace=True)

    # datatype
    df["price"] = df["price"].astype(float)
    df["area"] = df["area"].astype(float)
    df["bedroom"] = df["bedroom"].astype("Int64")
    df["bathroom"] = df["bathroom"].astype("Int64")
    df["total_posts"] = df["total_posts"].astype("Int64")

    df["posted_date"] = pd.to_datetime(
        df["posted_date"], format="%d/%m/%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df["expired_date"] = pd.to_datetime(
        df["expired_date"], format="%d/%m/%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    cols_order = [
        "title", "price", "price_unit", "area", "area_unit",
        "district", "city", "bedroom", "bathroom",
        "news_type", "posted_date", "expired_date",
        "seller", "total_posts", "url", "zalo_url"
    ]

    return df[cols_order]


def run():
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"{RAW_FILE} not found")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_json(RAW_FILE, dtype=str)
    df = clean_data(df)
    df = transform(df)

    df.to_csv(CLEAN_FILE, index=False, encoding="utf-8-sig")
    return df


if __name__ == "__main__":
    run()
