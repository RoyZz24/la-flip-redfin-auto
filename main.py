import json
import os
from datetime import datetime
import pandas as pd
from redfin_scraper import RedfinScraper
from geopy.distance import geodesic
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 配置区（你可以随时改）==================
ZIPS = ["91016", "91006", "91007", "91001", "91104", "91105", "91106", "91107"]  # Monrovia + 周边，越多越丰富
MAX_PRICE = 999999
MIN_LOT_SIZE = 1200
MIN_LIVING_SQFT = 1200
MIN_BEDS = 2
MIN_BATHS = 1.5
DISTANCE_MILE = 0.5

# ================== 核心代码 ==================
scraper = RedfinScraper()
scraper.setup(zip_database_path="zip_code_database.csv", multiprocessing=False)

today = datetime.now().strftime("%Y-%m-%d")

# 抓在售
scraper.scrape(zip_codes=ZIPS)
for_sale_raw = scraper.get_data(list(scraper.data.keys())[0]) if scraper.data else []
df_sale = pd.DataFrame(for_sale_raw)
print("=== ForSale RAW COLUMNS ===", df_sale.columns.tolist() if not df_sale.empty else "EMPTY")

# 抓已售（过去 1 年）
scraper.scrape(zip_codes=ZIPS, sold=True, sale_period="1yr")
sold_raw = scraper.get_data(list(scraper.data.keys())[0]) if scraper.data else []
df_sold = pd.DataFrame(sold_raw)
print("=== Sold RAW COLUMNS ===", df_sold.columns.tolist() if not df_sold.empty else "EMPTY")

# ================== 列名自动适配（解决 KeyError）=================
def normalize_columns(df):
    if df.empty:
        return df
    col_map = {
        'Price': 'price', 'ListingPrice': 'price', 'listingPrice': 'price', 'priceInfo.price': 'price',
        'SqFt': 'sqft', 'livingArea': 'sqft', 'squareFeet': 'sqft', 'sqft': 'sqft',
        'LotSize': 'lot_size', 'lotSize': 'lot_size', 'lot_size': 'lot_size',
        'Beds': 'beds', 'beds': 'beds',
        'Baths': 'baths', 'baths': 'baths',
        'YearBuilt': 'year_built', 'yearBuilt': 'year_built',
        'Description': 'description', 'description': 'description',
        'Photos': 'images', 'photos': 'images', 'image_urls': 'images',
        'Latitude': 'latitude', 'latitude': 'latitude',
        'Longitude': 'longitude', 'longitude': 'longitude',
        'PropertyType': 'property_type', 'propertyType': 'property_type'
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    
    # 安全填充缺失列
    for col in ['price', 'sqft', 'lot_size', 'beds', 'baths', 'latitude', 'longitude', 'description', 'images']:
        if col not in df.columns:
            df[col] = 0 if col in ['price', 'sqft', 'lot_size', 'beds', 'baths'] else ''
    return df

df_sale = normalize_columns(df_sale)
df_sold = normalize_columns(df_sold)

# ================== enrich（现在安全了）=================
def enrich_df(df, is_sold=False):
    df = df.copy()
    df['date_scraped'] = today
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce').fillna(1)
    df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(0)
    df['beds'] = pd.to_numeric(df['beds'], errors='coerce').fillna(0)
    df['baths'] = pd.to_numeric(df['baths'], errors='coerce').fillna(0)
    
    if not is_sold:
        df = df[
            (df['price'] <= MAX_PRICE) &
            (df['lot_size'] >= MIN_LOT_SIZE) &
            (df['sqft'] >= MIN_LIVING_SQFT) &
            (df['beds'] >= MIN_BEDS) &
            (df['baths'] >= MIN_BATHS) &
            (df.get('property_type', '').str.contains('Single Family|House', na=False))
        ]
    
    # fixer 关键词
    keywords = ['fixer', 'TLC', 'needs work', 'as-is', 'handyman', 'cosmetic', 'update', 'renovation']
    df['fixer_keywords'] = df['description'].fillna('').str.lower().apply(
        lambda x: ', '.join([k for k in keywords if k in x])
    )
    df['price_per_sqft'] = (df['price'] / df['sqft']).round(2)
    df['image_urls'] = df['images'].apply(lambda x: ' | '.join(x) if isinstance(x, list) else str(x))
    
    return df

df_sale = enrich_df(df_sale)
df_sold = enrich_df(df_sold, is_sold=True)

# ================== 自动计算 comps（超丰富）=================
def add_comps(row):
    if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')) or row.get('latitude') == 0:
        return 0, 0, ''
    nearby = df_sold[
        df_sold.apply(lambda x: geodesic(
            (row['latitude'], row['longitude']),
            (x.get('latitude', 0), x.get('longitude', 0))
        ).miles <= DISTANCE_MILE, axis=1)
    ]
    if len(nearby) == 0:
        return 0, 0, ''
    avg_pps = nearby['price_per_sqft'].mean()
    return len(nearby), round(avg_pps, 2), f"{len(nearby)} comps @ ${avg_pps}/sqft"

if not df_sale.empty:
    df_sale[['nearby_comps_count', 'avg_sold_price_per_sqft', 'comps_summary']] = df_sale.apply(add_comps, axis=1, result_type='expand')
    df_sale['est_margin'] = ((df_sale['avg_sold_price_per_sqft'] * df_sale['sqft'] - df_sale['price']) / df_sale['price'] * 100).round(1)

# ================== 写入 Google Sheet ==================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(creds)

sheet = client.open("LA_Flip_Redfin_Auto")
if not df_sale.empty:
    sheet.values_append("ForSale", "RAW", [df_sale.columns.tolist()] + df_sale.values.tolist())
if not df_sold.empty:
    sheet.values_append("Sold_Comps", "RAW", [df_sold.columns.tolist()] + df_sold.values.tolist())

print(f"✅ {today} 抓取完成！ForSale: {len(df_sale)} 条 | Sold: {len(df_sold)} 条")
