import http.client
import json
import csv
import os
import urllib.parse
import time
from datetime import datetime

API_KEY = "dd9df48260msh8e97b8e65dfc254p116fccjsne7a70df5df67"
HOST = "twitter241.p.rapidapi.com"
HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": HOST
}

INPUT_CSV = "input_users.csv"
OUTPUT_DIR = "data-user-rapid"

MAX_RETRY = 5
RETRY_DELAY = 3  # detik


# ============================================================
# LOAD IDENTIFIERS
# ============================================================
def load_csv_identifiers(csv_path):
    identifiers = []

    if not os.path.exists(csv_path):
        print(f"[ERROR] File {csv_path} tidak ditemukan.")
        return []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if "user_id_str" not in reader.fieldnames:
            print("[ERROR] CSV harus memiliki kolom: user_id_str")
            return []

        for row in reader:
            val = row.get("user_id_str", "").strip()
            if val:
                identifiers.append(val)

    print(f"[INFO] Total user_id_str ditemukan: {len(identifiers)}")
    return identifiers


# ============================================================
# FETCH USER VIA API
# ============================================================
def fetch_user(identifier):
    encoded = urllib.parse.quote(identifier, safe='')
    path = f"/get-users-v2?users={encoded}"

    for attempt in range(1, MAX_RETRY + 1):
        try:
            print(f"[GET] {path} (Attempt {attempt})")

            conn = http.client.HTTPSConnection(HOST, timeout=20)
            conn.request("GET", path, headers=HEADERS)
            res = conn.getresponse()

            status = res.status
            raw_data = res.read().decode("utf-8", errors="replace")

            # ============================
            # HANDLE RATE LIMIT
            # ============================
            if status == 429:
                print("[WARN] Rate limited (429). Menunggu sebelum retry...")
                time.sleep(RETRY_DELAY * attempt)
                continue

            # ============================
            # HANDLE SERVER ERROR
            # ============================
            if status >= 500:
                print(f"[WARN] Server error {status}. Retry...")
                time.sleep(RETRY_DELAY * attempt)
                continue

            # ============================
            # PARSE JSON
            # ============================
            try:
                data = json.loads(raw_data)
                return data
            except json.JSONDecodeError:
                print("[ERROR] JSON decode gagal.")
                return {
                    "error": "Invalid JSON",
                    "raw": raw_data
                }

        except Exception as e:
            print(f"[ERROR] Exception: {e}")
            time.sleep(RETRY_DELAY * attempt)

    return {
        "error": f"Gagal setelah {MAX_RETRY} percobaan",
        "user": identifier
    }


# ======= NEW: helper to normalize user object from various shapes =======

def extract_user_data(item):
    """Normalize a user-like object into a flat dict of fields."""
    if not isinstance(item, dict):
        return None

    user_obj = item
    # If wrapped as {'result': {...}} or {'result': [..]}, handle at caller
    # Dive into 'data' wrappers
    if 'data' in user_obj and isinstance(user_obj['data'], dict):
        data = user_obj['data']
        # if mapping of users, take first
        if isinstance(data, dict) and any(isinstance(v, dict) for v in data.values()):
            user_obj = next((v for v in data.values() if isinstance(v, dict)), data)
        else:
            user_obj = data

    # If it's a mapping of id -> user
    if isinstance(user_obj, dict) and all(isinstance(v, dict) for v in user_obj.values()):
        user_obj = next(iter(user_obj.values()))

    # result nested
    if 'result' in user_obj and isinstance(user_obj['result'], dict):
        candidate = user_obj['result']
    else:
        candidate = user_obj

    # legacy/user patterns
    if 'legacy' in candidate and isinstance(candidate['legacy'], dict):
        legacy = candidate['legacy']
    elif 'user' in candidate and isinstance(candidate['user'], dict):
        legacy = candidate['user']
    else:
        legacy = candidate

    # read fields with fallbacks
    user_id_str = legacy.get('id_str') or str(legacy.get('id') or '')
    screen_name = legacy.get('screen_name') or legacy.get('username') or ''
    name = legacy.get('name') or ''
    description = legacy.get('description') or legacy.get('bio') or ''
    location = legacy.get('location') or ''
    followers_count = legacy.get('followers_count') or legacy.get('followers') or ''
    friends_count = legacy.get('friends_count') or legacy.get('friends') or ''
    verified = legacy.get('verified', False)
    created_at = legacy.get('created_at') or ''
    profile_image_url = legacy.get('profile_image_url_https') or legacy.get('profile_image_url') or ''
    profile_banner_url = legacy.get('profile_banner_url') or ''
    url = legacy.get('url') or ''
    statuses_count = legacy.get('statuses_count') or legacy.get('statuses') or ''
    favourites_count = legacy.get('favourites_count') or legacy.get('favourites') or ''
    media_count = legacy.get('media_count') or ''

    return {
        'user_id': user_id_str,
        'screen_name': screen_name,
        'name': name,
        'description': description.replace('\n', ' ').strip(),
        'location': location,
        'followers_count': followers_count,
        'friends_count': friends_count,
        'statuses_count': statuses_count,
        'favourites_count': favourites_count,
        'media_count': media_count,
        'verified': verified,
        'created_at': created_at,
        'profile_image_url': profile_image_url,
        'profile_banner_url': profile_banner_url,
        'url': url
    }


# ======= UPDATED: save_to_csv now writes two files: raw and expanded =======

def save_to_csv(input_filename, results):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    category = os.path.splitext(os.path.basename(input_filename))[0]
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    raw_output_name = f"user_output_{category}_{timestamp}.csv"
    raw_output_path = os.path.join(OUTPUT_DIR, raw_output_name)

    expanded_output_name = f"user_expanded_{category}_{timestamp}.csv"
    expanded_output_path = os.path.join(OUTPUT_DIR, expanded_output_name)

    # write raw JSON file (same as before)
    with open(raw_output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "result"])

        for uid, result in results.items():
            compact_json = json.dumps(result, ensure_ascii=False).replace("\n", " ")
            writer.writerow([uid, compact_json])

    print(f"[DONE] Raw file berhasil disimpan: {raw_output_path}")

    # write expanded CSV with normalized fields
    fieldnames = [
        'user_id', 'screen_name', 'name', 'description', 'location',
        'followers_count', 'friends_count', 'statuses_count', 'favourites_count', 'media_count',
        'verified', 'created_at', 'profile_image_url', 'profile_banner_url', 'url'
    ]

    with open(expanded_output_path, "w", newline="", encoding="utf-8") as f2:
        writer2 = csv.DictWriter(f2, fieldnames=fieldnames)
        writer2.writeheader()

        for uid, result in results.items():
            # result may be an error dict or the API response dict
            if isinstance(result, dict) and 'error' in result:
                # write a minimal row with user_id and error in description
                writer2.writerow({
                    'user_id': uid,
                    'screen_name': '',
                    'name': '',
                    'description': f"ERROR: {result.get('error')}",
                    'location': '',
                    'followers_count': '',
                    'friends_count': '',
                    'statuses_count': '',
                    'favourites_count': '',
                    'media_count': '',
                    'verified': '',
                    'created_at': '',
                    'profile_image_url': '',
                    'profile_banner_url': '',
                    'url': ''
                })
                continue

            # parse actual response shapes
            normalized_rows = []
            # if top-level contains 'result' list
            if isinstance(result, dict) and 'result' in result and isinstance(result['result'], list):
                for item in result['result']:
                    normalized = extract_user_data(item)
                    if normalized:
                        normalized_rows.append(normalized)
            else:
                # try normalize the whole result
                normalized = extract_user_data(result)
                if normalized:
                    normalized_rows.append(normalized)

            # if nothing parsed, write a fallback row with raw string in description
            if not normalized_rows:
                writer2.writerow({
                    'user_id': uid,
                    'screen_name': '',
                    'name': '',
                    'description': f"UNPARSED: {str(result)[:200]}",
                    'location': '',
                    'followers_count': '',
                    'friends_count': '',
                    'statuses_count': '',
                    'favourites_count': '',
                    'media_count': '',
                    'verified': '',
                    'created_at': '',
                    'profile_image_url': '',
                    'profile_banner_url': '',
                    'url': ''
                })
                continue

            # write normalized rows (may be multiple if API returned list)
            for r in normalized_rows:
                # ensure user_id present
                if not r.get('user_id'):
                    r['user_id'] = uid
                # keep only fieldnames
                row = {k: r.get(k, '') for k in fieldnames}
                writer2.writerow(row)

    print(f"[DONE] Expanded file berhasil disimpan: {expanded_output_path}")


# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    print("[INFO] Membaca input_users.csv...")

    identifiers = load_csv_identifiers(INPUT_CSV)
    if not identifiers:
        print("[ERROR] Tidak ada user_id_str valid.")
        return

    results = {}

    for idx, uid in enumerate(identifiers, start=1):
        print(f"\n[REQUEST] {idx}/{len(identifiers)} → Fetch user {uid}")
        result = fetch_user(uid)
        results[uid] = result
        time.sleep(1)  # throttle biar aman dari rate limit

    save_to_csv(INPUT_CSV, results)
    print("\n[SELESAI] Semua proses telah selesai.\n")


if __name__ == "__main__":
    main()
