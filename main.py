import json
import os
import requests
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from geopy.distance import geodesic
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 配置区 ==================
ZIPS = ["91016", "91006", "91007", "91001", "91104", "91105", "91106", "91107"]  # 可随意加更多 zip
MAX_PRICE = 999999
MIN_LOT_SIZE = 1200
MIN_LIVING_SQFT = 1200
MIN_BEDS = 2
MIN_BATHS = 1.5
DISTANCE_MILE = 0.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

today = datetime.now().strftime("%Y-%m-%d")

def scrape_redfin(zip_code, is_sold=False):
    filter_str = f"property-type=house,max-price={MAX_PRICE},min-sqft={MIN_LIVING_SQFT},min-beds={MIN_BEDS},min-baths={MIN_BATHS}"
    if is_sold:
        filter_str += ",include=sold-1yr"
    url = f"https://www.redfin.com/zipcode/{zip_code}/filter/{filter_str}"
    print(f"正在抓取 {zip_code} {'已售' if is_sold else '在售'} → {url}")
    
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        print(f"❌ HTTP {resp.status_code}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script:
        print("❌ 未找到 __NEXT_DATA__")
        return pd.DataFrame()
    
    data = json.loads(script.string)
    try:
        homes = data["props"]["pageProps"]["initialState"]["homeData"]["homeSearch"]["results"]["homes"]
    except:
        homes = []
    
    df = pd.DataFrame(homes)
    if df.empty:
        return df
    # 展开关键字段
    df["address"] = df["address"].apply(lambda x: x.get("line") if isinstance(x, dict) else "")
    df["price"] = pd.to_numeric(df.get("price") or df.get("soldPrice"), errors="coerce").fillna(0)
    df["sqft"] = pd.to_numeric(df.get("sqft"), errors="coerce").fillna(0)
    df["lot_size"] = pd.to_numeric(df.get("lotSize"), errors="coerce").fillna(0)
    df["beds"] = pd.to_numeric(df.get("beds"), errors="coerce").fillna(0)
    df["baths"] = pd.to_numeric(df.get("baths"), errors="coerce").fillna(0)
    df["year_built"] = pd.to_numeric(df.get("yearBuilt"), errors="coerce").fillna(0)
    df["description"] = df.get("description", "")
    df["image_urls"] = df.get("photos", "").apply(lambda x: " | ".join([p.get("url", "") for p in x]) if isinstance(x, list) else "")
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    df["link"] = "https://www.redfin.com" + df.get("url", "")
    df["property_type"] = df.get("propertyType", "")
    return df

# 抓在售 + 已售
df_sale = pd.concat([scrape_redfin(z) for z in ZIPS], ignore_index=True)
df_sold = pd.concat([scrape_redfin(z, is_sold=True) for z in ZIPS], ignore_index=True)

print(f"抓到在售 {len(df_sale)} 条，已售 {len(df_sold)} 条")

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

