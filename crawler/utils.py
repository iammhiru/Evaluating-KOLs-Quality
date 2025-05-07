import os
import json
import csv
from dotenv import load_dotenv
import base64
import re
import urllib.parse

load_dotenv()

def save_to_json(data, directory, filename):
    os.makedirs(directory, exist_ok=True)
    with open(f"{directory}/{filename}", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

def decode_comment_base64(comment_str):
    try:
        comment_str = urllib.parse.unquote(comment_str)
        decoded = base64.b64decode(comment_str).decode("utf-8")
        match = re.search(r'comment:\d+_(\d+)', decoded)
        return match.group(1) if match else None
    except Exception as e:
        print(f"Decode error: {e}")
        return None
