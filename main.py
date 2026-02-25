import json
import os
from datetime import datetime
import pandas as pd
from redfin_scraper import RedfinScraper
from geopy.distance import geodesic
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 配置区（改这里就行）==================
ZIPS = ["91016", "91006", "91007", "91001", "91104", "91105", "91106", "91107"]  # Monrovia + 周边，越多房源越丰富
MAX_PRICE = 999999
MIN_LOT_SIZE = 1200          # 占地至少 1200 sqft
MIN_LIVING_SQFT = 1200       # 同时要求 living area >=1200
MIN_BEDS = 2
MIN_BATHS = 1.5
DISTANCE_MILE = 0.5          # comps 有效距离

# ================== 核心代码 ==================
scraper = RedfinScraper()
scraper.setup(zip_database_path="zip_code_database.csv", multiprocessing=False)

today = datetime.now().strftime("%Y-%m-%d")

# 抓在售
scraper.scrape(zip_codes=ZIPS)
for_sale_raw = scraper.get_data(list(scraper.data.keys())[0])  # 取最新一次
df_sale = pd.DataFrame(for_sale_raw)

# 抓已售（过去 1 年）
scraper.scrape(zip_codes=ZIPS, sold=True, sale_period="1yr")
sold_raw = scraper.get_data(list(scraper.data.keys())[0])
df_sold = pd.DataFrame(sold_raw)

# 过滤 + 丰富数据
def enrich_df(df, is_sold=False):
    df = df.copy()
    df['date_scraped'] = today
    # 基本过滤
    if not is_sold:
        df = df[
            (df['price'] <= MAX_PRICE) &
            (df.get('lot_size', 0) >= MIN_LOT_SIZE) &
            (df.get('sqft', 0) >= MIN_LIVING_SQFT) &
            (df.get('beds', 0) >= MIN_BEDS) &
            (df.get('baths', 0) >= MIN_BATHS) &
            (df.get('property_type', '').str.contains('Single Family', na=False))
        ]
    # fixer 关键词检测
    keywords = ['fixer', 'TLC', 'needs work', 'as-is', 'handyman', 'cosmetic', 'update', 'renovation']
    df['fixer_keywords'] = df['description'].fillna('').str.lower().apply(
        lambda x: ', '.join([k for k in keywords if k in x])
    )
    df['price_per_sqft'] = df['price'] / df['sqft'].replace(0, 1)
    df['image_urls'] = df['images'].apply(lambda x: ' | '.join(x) if isinstance(x, list) else x)
    return df

df_sale = enrich_df(df_sale)
df_sold = enrich_df(df_sold, is_sold=True)

# 自动计算附近 comps（超丰富！）
def add_comps(row):
    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
        return 0, 0, ''
    nearby = df_sold[
        df_sold.apply(lambda x: geodesic(
            (row['latitude'], row['longitude']),
            (x['latitude'], x['longitude'])
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
sheet.values_append("ForSale", "RAW", df_sale.values.tolist()) if not df_sale.empty else None
sheet.values_append("Sold_Comps", "RAW", df_sold.values.tolist()) if not df_sold.empty else None

print(f"✅ {today} 抓取完成！ForSale: {len(df_sale)} 条 | Sold: {len(df_sold)} 条")
