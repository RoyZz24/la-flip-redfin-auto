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
ZIPS = ["91505", "91214"]
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

def scrape_redfin(zip_code, is_sold=False, retries=3):
    for attempt in range(retries):
        try:
            driver = get_driver()
            filter_str = f"property-type=house,max-price={MAX_PRICE},min-sqft={MIN_LIVING_SQFT},min-beds={MIN_BEDS},min-baths={MIN_BATHS}"
            if is_sold:
                filter_str += ",include=sold-1yr"
            url = f"https://www.redfin.com/zipcode/{zip_code}/filter/{filter_str}"
            print(f"第{attempt+1}次尝试抓取 {zip_code} {'已售' if is_sold else '在售'} → {url}")
            
            driver.get(url)
            time.sleep(25 + random.uniform(5, 15))
            for _ in range(10):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(6 + random.uniform(0, 3))
            
            WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.HomeCardContainer, .HomeCardContainer, [data-rf-test-id='property-card'], .card")))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            driver.quit()
            
            cards = soup.find_all("div", class_="HomeCardContainer") or soup.find_all(attrs={"data-rf-test-id": "property-card"}) or soup.find_all("div", class_="card")
            print(f"找到 {len(cards)} 个房源卡片")
            
            data = []
            for i, card in enumerate(cards):
                try:
                    text = card.get_text(separator=" | ", strip=True)
                    if i < 3: print(f"  第{i+1}条卡片文本: {text[:250]}...")  # debug
                    
                    link_tag = card.find("a", href=True)
                    link = "https://www.redfin.com" + link_tag["href"] if link_tag else ""
                    
                    # 加强正则匹配真实地址、beds、baths、sqft
                    addr_match = re.search(r'(\d+\s+[A-Za-z0-9\s,]+(?:St|Ave|Rd|Blvd|Ln|Dr|Way|Ct|Pl))', text)
                    address = addr_match.group(1) if addr_match else "未知地址"
                    
                    beds_match = re.search(r'(\d+)\s*beds?', text, re.IGNORECASE)
                    baths_match = re.search(r'(\d+\.?\d*)\s*baths?', text, re.IGNORECASE)
                    sqft_match = re.search(r'(\d{1,4}(?:,\d{3})?)\s*sqft', text, re.IGNORECASE)
                    beds = int(beds_match.group(1)) if beds_match else 0
                    baths = float(baths_match.group(1)) if baths_match else 0
                    sqft = int(sqft_match.group(1).replace(',', '')) if sqft_match else 0
                    
                    price_match = re.search(r'\$(\d{1,3}(?:,\d{3})*)', text)
                    price = int(price_match.group(1).replace(',', '')) if price_match else 0
                    
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
            print(f"→ 本 zip 实际提取到 {len(data)} 条有效数据")
            return pd.DataFrame(data)
        except:
            print(f"第{attempt+1}次失败，重试中...")
            time.sleep(10)
            continue
    return pd.DataFrame()

# 抓取 + enrich
df_sale = pd.concat([scrape_redfin(z) for z in ZIPS], ignore_index=True)
df_sold = pd.concat([scrape_redfin(z, is_sold=True) for z in ZIPS], ignore_index=True)

print(f"✅ 总抓到在售 {len(df_sale)} 条，已售 {len(df_sold)} 条")

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
    
    existing = worksheet.get_all_values()
    if not existing or existing[0] != df.columns.tolist():
        worksheet.append_row(df.columns.tolist())
    
    # 只添加数据（去重）
    if not df.empty:
        df = df.drop_duplicates(subset=['address', 'price'])
        worksheet.append_rows(df.values.tolist(), value_input_option='RAW')

print(f"🎉 {today} 写入完成！请刷新Sheet查看真实房源")
