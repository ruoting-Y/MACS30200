# This script scrapes Weibo posts based on event keywords and extracts user interactions for network analysis

import os
import json
import csv
import re
import time
import requests
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime, timedelta
from lxml import etree
from tqdm import tqdm

# ===== CONFIGURATION ===== #
EVENT_KEYWORDS = ["铁链女", "金星封杀", "俄乌战争"]
SINCE_DATE = "2023-06-01"
UNTIL_DATE = "2023-06-10"
PAGE_LIMIT = 50
OUT_DIR = "research_output"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

# ===== UTILITIES ===== #
def clean_text(html):
    text = etree.HTML(html).xpath("string(.)")
    return text.strip()

def extract_hashtags(text):
    return re.findall(r"#(.*?)#", text)

def write_csv(path, rows, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

# ===== MAIN SCRAPER CLASS ===== #
class WeiboResearchScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.interactions = []
        self.user_hashtags = defaultdict(list)
        self.post_data = []

    def scrape_keyword(self, keyword):
        for page in range(1, PAGE_LIMIT + 1):
            params = {
                "containerid": f"100103type=1&q={quote(keyword)}",
                "page_type": "searchall",
                "page": page
            }
            try:
                resp = self.session.get("https://m.weibo.cn/api/container/getIndex", params=params)
                data = resp.json()
                cards = data.get("data", {}).get("cards", [])
                if not cards:
                    break
                for card in cards:
                    if card.get("card_type") == 9:
                        self.handle_post(card.get("mblog"))
                time.sleep(1)
            except Exception as e:
                print(f"Error on page {page}: {e}")
                continue

    def handle_post(self, mblog):
        if not mblog:
            return
        created_at = self.standardize_date(mblog.get("created_at"))
        if not (SINCE_DATE <= created_at <= UNTIL_DATE):
            return

        user = mblog.get("user", {})
        uid = user.get("id")
        screen_name = user.get("screen_name")
        text = clean_text(mblog.get("text", ""))
        hashtags = extract_hashtags(text)

        self.post_data.append([uid, screen_name, created_at, text, ",".join(hashtags)])
        self.user_hashtags[uid].extend(hashtags)

        # Handle retweets
        if mblog.get("retweeted_status"):
            target_user = mblog["retweeted_status"].get("user", {}).get("id")
            if target_user:
                self.interactions.append([uid, target_user, "retweet", created_at])

        # Handle mentions
        mentions = re.findall(r"@([\u4e00-\u9fa5A-Za-z0-9_]+)", text)
        for name in mentions:
            self.interactions.append([uid, name, "mention", created_at])

    def standardize_date(self, created_at):
        # crude conversion for demo purposes
        try:
            if "分钟前" in created_at or "刚刚" in created_at:
                return datetime.now().strftime("%Y-%m-%d")
            if "小时" in created_at:
                return datetime.now().strftime("%Y-%m-%d")
            if "昨天" in created_at:
                return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            if "-" in created_at:
                return created_at[:10]
            return ""
        except:
            return ""

    def save(self):
        write_csv(os.path.join(OUT_DIR, "posts.csv"), self.post_data,
                  ["user_id", "screen_name", "date", "text", "hashtags"])
        write_csv(os.path.join(OUT_DIR, "interactions.csv"), self.interactions,
                  ["source_user", "target_user", "type", "date"])
        with open(os.path.join(OUT_DIR, "user_hashtags.json"), "w", encoding="utf-8") as f:
            json.dump(self.user_hashtags, f, ensure_ascii=False, indent=2)

# ===== RUN ===== #
if __name__ == "__main__":
    scraper = WeiboResearchScraper()
    for kw in EVENT_KEYWORDS:
        print(f"Scraping keyword: {kw}")
        scraper.scrape_keyword(kw)
    scraper.save()
    print("✅ Done.")
