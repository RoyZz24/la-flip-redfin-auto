import json
import os
import time
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from geopy.distance import geodesic
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 配置区（可随时改）==================
ZIPS = ["91016", "91006", "91007", "91001", "91104", "91105", "91106", "91107"]  # Monrovia + 周边，越多越丰富
MAX_PRICE = 999999
MIN_LIVING_SQFT = 1200
MIN_BEDS = 2
MIN_BATHS = 1.5
DISTANCE_MILE = 0.5

today = datetime.now().strftime("%Y-%m-%d")

def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scrape_redfin(zip_code, is_sold=False):
    driver = get_driver()
    filter_str = f"property-type=house,max-price={MAX_PRICE},min-sqft={MIN_LIVING_SQFT},min-beds={MIN_BEDS},min-baths={MIN_BATHS}"
    if is_sold:
        filter_str += ",include=sold-1yr"
    url = f"https://www.redfin.com/zipcode/{zip_code}/filter/{filter_str}"
    print(f"正在抓取 {zip_code} {'已售' if is_sold else '在售'} → {url}")
    
    driver.get(url)
    # 等待卡片加载（2026 最新稳定方式）
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.HomeCardContainer, [data-rf-test-id='property-card']")))
    except:
        print("等待超时")
    # 滚动加载更多
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(6)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    cards = soup.find_all("div", class_="HomeCardContainer") or soup.find_all("[data-rf-test-id='property-card']")
    print(f"找到 {len(cards)} 个房源卡片")
    
    data = []
    for card in cards:
        try:
            link_tag = card.find("a", class_="link-and-anchor") or card.find("a")
            link = "https://www.redfin.com" + link_tag["href"] if link_tag else ""
            
            address = card.find("div", class_="bp-Homecard__Address")
            address = address.text.strip() if address else ""
            
            price_tag = card.find("span", class_="bp-Homecard__Price--value") or card.find("span", class_="Homecard__Price--value")
            price = int(''.join(filter(str.isdigit, price_tag.text))) if price_tag else 0
            
            stats = card.find_all("span", class_="bp-Homecard__Stats--value")
            beds = int(stats[0].text) if len(stats) > 0 else 0
            baths = float(stats[1].text) if len(stats) > 1 else 0
            sqft = int(''.join(filter(str.isdigit, stats[2].text))) if len(stats) > 2 else 0
            
            img = card.find("img")
            image_url = img["src"] if img else ""
            
            data.append({
                "date_scraped": today,
                "address": address,
                "price": price,
                "sqft": sqft,
                "beds": beds,
                "baths": baths,
                "link": link,
                "image_urls": image_url,
                "description": "",   # 后续可升级抓详情页
                "fixer_keywords": ""
            })
        except:
            continue
    return pd.DataFrame(data)

# 抓取在售 + 已售
df_sale = pd.concat([scrape_redfin(z) for z in ZIPS], ignore_index=True)
df_sold = pd.concat([scrape_redfin(z, is_sold=True) for z in ZIPS], ignore_index=True)

print(f"✅ 抓到在售 {len(df_sale)} 条，已售 {len(df_sold)} 条")

# enrich + 自动 comps + margin（解决你的两个难点）
def enrich_df(df, is_sold=False):
    if df.empty:
        return df
    df = df.copy()
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce').fillna(1)
    df['price_per_sqft'] = (df['price'] / df['sqft']).round(2)
    
    if not is_sold:
        df = df[(df['price'] <= MAX_PRICE) & (df['sqft'] >= MIN_LIVING_SQFT) & (df['beds'] >= MIN_BEDS) & (df['baths'] >= MIN_BATHS)]
    
    # fixer 关键词（description 为空时可手动点 link 看）
    keywords = ['fixer', 'TLC', 'needs work', 'as-is', 'handyman', 'update', 'renovation']
    df['fixer_keywords'] = ""  # 后续可升级抓 description
    
    return df

df_sale = enrich_df(df_sale)
df_sold = enrich_df(df_sold, is_sold=True)

# 简单版 comps（用全已售平均，后面可升级距离）
if not df_sale.empty and not df_sold.empty:
    avg_pps = df_sold['price_per_sqft'].mean()
    df_sale['avg_sold_price_per_sqft'] = round(avg_pps, 2)
    df_sale['est_margin'] = ((avg_pps * df_sale['sqft'] - df_sale['price']) / df_sale['price'] * 100).round(1)
    df_sale['nearby_comps_count'] = len(df_sold)

# 写入 Google Sheet（自动追加 + 表头）
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(creds)

sheet = client.open("LA_Flip_Redfin_Auto")
if not df_sale.empty:
    sheet.values_append("ForSale", "RAW", [df_sale.columns.tolist()] + df_sale.values.tolist())
if not df_sold.empty:
    sheet.values_append("Sold_Comps", "RAW", [df_sold.columns.tolist()] + df_sold.values.tolist())

print(f"🎉 {today} 抓取完成！ForSale: {len(df_sale)} | Sold: {len(df_sold)}")
