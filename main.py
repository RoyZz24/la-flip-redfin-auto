import json
import os
import time
import random
import re
import numpy as np
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
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 配置区 ==================
ZIPS = ["burbank-ca", "la-crescenta-ca", "la-crescenta-montrose-ca"]  # Zillow 城市格式
MAX_PRICE = 999999
MIN_LIVING_SQFT = 1200
MIN_BEDS = 2
MIN_BATHS = 1.5

today = datetime.now().strftime("%Y-%m-%d")

def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scrape_zillow(city, is_sold=False, retries=3):
    for attempt in range(retries):
        try:
            driver = get_driver()
            url = f"https://www.zillow.com/{city}/houses/{MIN_BEDS}_beds/{MIN_BATHS}_baths/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22usersSearchTerm%22%3A%22{city}%22%2C%22filterState%22%3A%7B%22price%22%3A%7B%22max%22%3A{MAX_PRICE}%7D%2C%22sqft%22%3A%7B%22min%22%3A{MIN_LIVING_SQFT}%7D%7D%7D"
            if is_sold:
                url = url.replace("houses", "sold")
            print(f"第{attempt+1}次尝试抓取 {city} {'已售' if is_sold else '在售'} → {url}")
            
            driver.get(url)
            time.sleep(25 + random.uniform(5, 15))
            for _ in range(10):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(6 + random.uniform(0, 3))
            
            WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-card, article")))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            driver.quit()
            
            cards = soup.find_all("div", class_="list-card") or soup.find_all("article")
            print(f"找到 {len(cards)} 个房源卡片")
            
            data = []
            for card in cards:
                try:
                    link_tag = card.find("a")
                    link = "https://www.zillow.com" + link_tag["href"] if link_tag else ""
                    
                    address = card.find("address").text.strip() if card.find("address") else "未知地址"
                    
                    price_tag = card.find("span", class_="list-card-price")
                    price = int(''.join(filter(str.isdigit, price_tag.text))) if price_tag else 0
                    
                    stats = card.find_all("li")
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
                        "fixer_keywords": ""
                    })
                except:
                    continue
            print(f"→ 本城市实际提取到 {len(data)} 条有效数据")
            return pd.DataFrame(data)
        except:
            print(f"第{attempt+1}次失败，重试中...")
            time.sleep(10)
            continue
    return pd.DataFrame()

# 抓取
df_sale = pd.concat([scrape_zillow(z) for z in ZIPS], ignore_index=True)
df_sold = pd.concat([scrape_zillow(z, is_sold=True) for z in ZIPS], ignore_index=True)

print(f"✅ 总抓到在售 {len(df_sale)} 条，已售 {len(df_sold)} 条")

# enrich
def enrich_df(df, is_sold=False):
    if df.empty:
        return df
    df = df.copy()
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce').fillna(0)
    df['beds'] = pd.to_numeric(df['beds'], errors='coerce').fillna(0)
    df['baths'] = pd.to_numeric(df['baths'], errors='coerce').fillna(0)
    df['price_per_sqft'] = (df['price'] / df['sqft'].replace(0, 1)).round(2)
    if not is_sold:
        df = df[df['price'] > 0]
    return df

df_sold = enrich_df(df_sold, is_sold=True)
df_sale = enrich_df(df_sale)

if not df_sale.empty and not df_sold.empty:
    avg_pps = df_sold['price_per_sqft'].mean()
    df_sale['avg_sold_price_per_sqft'] = round(avg_pps, 2)
    df_sale['est_margin'] = ((avg_pps * df_sale['sqft'] - df_sale['price']) / df_sale['price'] * 100).round(1)
    df_sale['nearby_comps_count'] = len(df_sold)

# 写入（只加一次表头 + 去重）
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(creds)

sheet = client.open("LA_Flip_Redfin_Auto")

for tab_name, df in [("ForSale", df_sale), ("Sold_Comps", df_sold)]:
    try:
        worksheet = sheet.worksheet(tab_name)
    except:
        worksheet = sheet.add_worksheet(title=tab_name, rows=1000, cols=20)
    if not worksheet.get_all_values():
        worksheet.append_row(df.columns.tolist())
    if not df.empty:
        df = df.drop_duplicates(subset=['address', 'price'])
        worksheet.append_rows(df.values.tolist(), value_input_option='RAW')

print(f"🎉 {today} 写入完成！请刷新Sheet查看真实房源")
