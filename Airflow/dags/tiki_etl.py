import pandas as pd
import requests
import json
import math

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Accept-Language": 'en-US,en;q=0.9',
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://tiki.vn/",
    "From": "",
    "af-ac-enc-dat": "",
    "x-api-source": "pc"
}

URL = "https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"

response = requests.get(URL, headers=HEADERS)
data = response.json()

groups = data["menu_block"]["items"]
group_list = []

for group in groups:
    text = group["text"]
    link = group["link"]
    # Extract category ID from the link
    group_id = link.split("/")[-1][1:]  
    group_list.append([group_id, text])

groups = pd.DataFrame(group_list, columns=["Group_ID", "Group_Name"])