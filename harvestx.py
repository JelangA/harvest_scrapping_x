import subprocess
import json
import time
import os
from datetime import datetime

twitter_auth_token = "bd11356d590237b72067800284e1c03346f2cb07"
tweet_count = 50

def run_tweet_harvest(output_filename, keyword):
    command = [
        "node",
        "lib/dist/bin.js",
        "-o", f"{output_filename}.csv",
        "-s", f'{keyword} lang:in',
        "-l", str(tweet_count),
        "--token", twitter_auth_token
    ]

    print(f" Menjalankan: {' '.join(command)}")

    try:
        subprocess.run(command, check=True)
        print(f"Selesai: {output_filename}.csv")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def run_batch(batch_number, data):
    for category, tags in data.items():
        print(f"\nKategori: {category.upper()} | Batch {batch_number}")

        for index, tag in enumerate(tags, start=1):
            output_filename = f"{category}_{batch_number}_{index}"
            print(f"Tag {index}: {tag}")

            run_tweet_harvest(output_filename, tag)

            print("Delay 30 detik...\n")
            time.sleep(30)

        print(f"Kategori '{category}' batch {batch_number} selesai!\n")

def main():
    with open("keyword_input.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    batch = 1

    while True:
        print(f"\n=== MULAI BATCH {batch} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===\n")

        run_batch(batch, data)

        print(f"Batch {batch} selesai! Menunggu 5 menit sebelum batch berikutnya...\n")
        try:
            subprocess.run(["python", os.path.join("selection.py"), str(batch)], check=True)
            print(f"Merge selesai batch {batch}!\n")
        except Exception as e:
            print(f"Merge gagal: {e}")
        time.sleep(5 * 60)  # 5 menit
        print(f"Mulai proses merge hasil batch {batch}...")


        batch += 1

if __name__ == "__main__":
    main()
