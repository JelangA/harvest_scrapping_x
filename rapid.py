import http.client
import json
import csv
import os
from typing import List, Dict, Any, Optional

API_KEY = "dd9df48260msh8e97b8e65dfc254p116fccjsne7a70df5df67"
HOST = "twitter-x.p.rapidapi.com"
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': HOST
}

# === Folder output ===
OUTPUT_DIR = "data-tweet-rapid"

def fetch_tweets(keyword: str) -> Optional[Dict[str, Any]]:
    """Ambil data tweet dari API berdasarkan keyword."""
    try:
        conn = http.client.HTTPSConnection(HOST)
        path = f"/search/?query={keyword}&lang=id&section=latest&limit=20"
        conn.request("GET", path, headers=HEADERS)
        res = conn.getresponse()
        raw_data = res.read()
        conn.close()
        return json.loads(raw_data.decode("utf-8"))
    except Exception as e:
        print(f"❌ Gagal fetch untuk '{keyword}': {e}")
        return None


def extract_tweet_data(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ekstrak data tweet dari struktur nested JSON."""
    try:
        if 'itemContent' in item:
            tweet_results = item['itemContent'].get('tweet_results', {}).get('result', {})
        elif 'content' in item:
            tweet_results = item['content'].get('itemContent', {}).get('tweet_results', {}).get('result', {})
        else:
            tweet_results = item

        legacy = tweet_results.get('legacy', {})
        user_results = tweet_results.get('core', {}).get('user_results', {}).get('result', {})
        user_legacy = user_results.get('legacy', {})

        lang = legacy.get('lang', '')
        if lang != 'in':
            return None  # hanya tweet Bahasa Indonesia

        tweet_id = tweet_results.get('rest_id', '')
        username = user_legacy.get('screen_name', '')
        media = legacy.get('entities', {}).get('media', [])
        image_url = media[0].get('media_url_https', '') if media else ''

        return {
            'conversation_id_str': legacy.get('conversation_id_str', ''),
            'created_at': legacy.get('created_at', ''),
            'favorite_count': legacy.get('favorite_count', 0),
            'full_text': legacy.get('full_text', '').replace('\n', ' ').strip(),
            'id_str': tweet_id,
            'image_url': image_url,
            'in_reply_to_screen_name': legacy.get('in_reply_to_screen_name', ''),
            'lang': lang,
            'like_count': legacy.get('favorite_count', 0),
            'location': user_legacy.get('location', ''),
            'quote_count': legacy.get('quote_count', 0),
            'reply_count': legacy.get('reply_count', 0),
            'retweet_count': legacy.get('retweet_count', 0),
            'tweet_url': f"https://twitter.com/{username}/status/{tweet_id}" if tweet_id and username else '',
            'user_id_str': legacy.get('user_id_str', ''),
            'username': username
        }
    except Exception:
        return None


def parse_json_response(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Ambil semua item tweet dari struktur JSON kompleks."""
    data_to_process = []
    if isinstance(json_data, dict):
        if 'data' in json_data:
            timeline = json_data['data'].get('search_by_raw_query', {}).get('search_timeline', {}).get('timeline', {})
            for instr in timeline.get('instructions', []):
                if instr.get('type') == 'TimelineAddEntries':
                    for entry in instr.get('entries', []):
                        if 'tweet-' in entry.get('entryId', ''):
                            data_to_process.append(entry.get('content', {}))
        elif 'results' in json_data:
            data_to_process = json_data['results']
        else:
            data_to_process = [json_data]
    return data_to_process


def save_to_csv(filename: str, tweets: List[Dict[str, Any]]):
    """Simpan hasil tweet ke file CSV (append jika sudah ada)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    file_exists = os.path.exists(filepath)
    with open(filepath, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'conversation_id_str', 'created_at', 'favorite_count', 'full_text', 'id_str',
            'image_url', 'in_reply_to_screen_name', 'lang', 'like_count', 'location',
            'quote_count', 'reply_count', 'retweet_count', 'tweet_url', 'user_id_str', 'username'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for tweet in tweets:
            writer.writerow(tweet)


def main():
    # === Baca keyword_input.json ===
    with open("keyword_input.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    for category, keywords in data.items():
        print(f"\n=== 🔍 KATEGORI: {category.upper()} ===")
        filename = f"{category}.csv"

        for keyword in keywords:
            print(f"➡️  Mencari: {keyword}")
            json_data = fetch_tweets(keyword)
            if not json_data:
                continue

            data_to_process = parse_json_response(json_data)
            results = []

            for item in data_to_process:
                tweet_data = extract_tweet_data(item)
                if tweet_data:
                    results.append(tweet_data)

            if results:
                save_to_csv(filename, results)
                print(f"   ✅ {len(results)} tweet disimpan ke {os.path.join(OUTPUT_DIR, filename)}")
            else:
                print(f"   ⚠️ Tidak ada tweet berbahasa Indonesia untuk '{keyword}'")

    print("\n🎉 Semua kategori selesai diproses!")


if __name__ == "__main__":
    main()
