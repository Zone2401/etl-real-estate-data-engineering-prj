import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("/opt/airflow/data")
RAW_FILE = DATA_DIR / "raw_data.json"
CLEAN_FILE = DATA_DIR / "clean_data.csv"

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    
    
    df.replace(["None", "Giá thỏa thuận", "", "NaN", "nan"], np.nan, inplace=True)
    
    
    text_cols = ["price", "area", "address", "bedroom", "bathroom", "news_type", "total_posts"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")
            
    df.drop_duplicates(inplace=True)
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Định nghĩa cấu trúc cột đầu ra cố định
    cols_order = [
        "title", "price", "price_unit", "area", "area_unit",
        "district", "city", "bedroom", "bathroom",
        "news_type", "posted_date", "expired_date",
        "seller", "total_posts", "url", "zalo_url"
    ]

    # Kiểm tra nếu DataFrame rỗng (Trường hợp không có bài đăng trong ngày)
    if df.empty:
        return pd.DataFrame(columns=cols_order)

    df = df.copy()

    # Price & Area
    df["price"] = df["price"].astype(str).str.replace(",", ".", regex=False)
    
    price_ext = df["price"].str.extract(r'(?P<p_val>\d+\.?\d*)\s*(?P<p_unit>.*)')
    df["price"] = price_ext["p_val"]
    df["price_unit"] = price_ext["p_unit"]

    area_ext = df["area"].astype(str).str.extract(r'(?P<a_val>\d+\.?\d*)\s*(?P<a_unit>.*)')
    df["area"] = area_ext["a_val"]
    df["area_unit"] = area_ext["a_unit"]

    # Address
    address_split = df["address"].astype(str).str.split(",", n=1, expand=True)
    df["district"] = address_split[0].str.strip()
    df["city"] = address_split[1].str.strip() if address_split.shape[1] > 1 else ""

    # Clean các từ thừa
    df["bedroom"] = df["bedroom"].astype(str).str.replace("Phòng ngủ", "", regex=False)
    df["bathroom"] = df["bathroom"].astype(str).str.replace("WC", "", regex=False)
    df["news_type"] = df["news_type"].astype(str).str.replace("Tin", "", regex=False)
    df["total_posts"] = df["total_posts"].astype(str).str.extract(r"(\d+)")

    # Sử dụng errors='coerce' để nếu gặp dữ liệu lỗi sẽ chuyển thành NaN thay vì crash
    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    mask_nghin = df["price_unit"].str.contains("nghìn", na=False)
    df.loc[mask_nghin, "price"] = df.loc[mask_nghin, "price"] / 1000
    df.loc[mask_nghin, "price_unit"] = "triệu/tháng"
    df["area"] = pd.to_numeric(df["area"], errors='coerce')
    
    # Dùng kiểu Int64 (viết hoa I) của Pandas để hỗ trợ chứa cả số nguyên và NaN
    df["bedroom"] = pd.to_numeric(df["bedroom"], errors='coerce').astype("Int64")
    df["bathroom"] = pd.to_numeric(df["bathroom"], errors='coerce').astype("Int64")
    df["total_posts"] = pd.to_numeric(df["total_posts"], errors='coerce').astype("Int64")

    # Xử lý ngày tháng
    for date_col in ["posted_date", "expired_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")

    # Đảm bảo đủ các cột output (tránh lỗi KeyError nếu thiếu cột từ nguồn)
    for col in cols_order:
        if col not in df.columns:
            df[col] = np.nan

    return df[cols_order]

def run():
    if not RAW_FILE.exists():
        print(f"Cảnh báo: Không tìm thấy file {RAW_FILE}")
        return

    try:
        # Đọc dữ liệu, ép kiểu string ngay từ đầu để tránh Pandas tự đoán sai
        df = pd.read_json(RAW_FILE, dtype=str)
    except Exception as e:
        print(f"Lỗi đọc file JSON: {e}")
        df = pd.DataFrame()

    # Thực hiện Pipeline
    df = clean_data(df)
    df = transform(df)

    # Lưu kết quả an toàn với encoding utf-8-sig (hỗ trợ tiếng Việt trên Excel)
    df.to_csv(CLEAN_FILE, index=False, encoding="utf-8-sig")
    print(f"Hoàn thành! Đã lưu {len(df)} dòng dữ liệu vào {CLEAN_FILE}")
    return df

if __name__ == "__main__":
    run()