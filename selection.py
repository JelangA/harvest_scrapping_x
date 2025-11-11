import pandas as pd
import glob
import os
import sys

data_folder = "tweets-data"
output_folder = os.path.join(data_folder, "result")

if len(sys.argv) < 2:
    print("❌ Parameter batch tidak diberikan")
    print("Gunakan: python selection.py <batch_number>")
    sys.exit(1)

batch_number = sys.argv[1]
batch_folder = os.path.join(output_folder, f"batch_{batch_number}")
os.makedirs(batch_folder, exist_ok=True)

pattern = os.path.join(data_folder, f"*_{batch_number}_*.csv")
files = glob.glob(pattern)

if not files:
    print(f"⚠️ Tidak ada file CSV untuk batch {batch_number}")
    sys.exit()

print(f"\n🔎 File ditemukan untuk batch {batch_number}:")
for f in files:
    print(" →", f)

categories = {
    "sosial": "sosial.csv",
    "ekonomi": "ekonomi.csv",
    "politik": "politik.csv"
}

dfs = {key: [] for key in list(categories.keys()) + ["other"]}

for file in files:
    if os.path.getsize(file) == 0:
        print(f"[SKIP] {file} kosong")
        continue

    filename = os.path.basename(file).lower()
    matched = False

    try:
        df = pd.read_csv(file)
    except Exception as e:
        print(f"[ERROR] Tidak bisa baca {file}: {e}")
        continue

    for key in categories:
        if key in filename:
            dfs[key].append(df)
            print(f"[MATCH] {file} → {key}")
            matched = True
            break

    if not matched:
        dfs["other"].append(df)
        print(f"[OTHER] {file} → other")

def save_category(key, df_list):
    if not df_list:
        print(f"⚠️ Tidak ada data untuk kategori {key}")
        return

    print(f"\n📌 Merge kategori: {key}")
    merged = pd.concat(df_list, ignore_index=True)

    if "id_str" in merged.columns:
        merged.drop_duplicates(subset="id_str", inplace=True)

    output_name = categories.get(key, "other.csv")
    output_path = os.path.join(batch_folder, output_name)

    if os.path.exists(output_path):
        old_df = pd.read_csv(output_path)
        merged = pd.concat([old_df, merged], ignore_index=True).drop_duplicates(subset="id_str")

    merged.to_csv(output_path, index=False)

    print(f"✅ Saved: {output_path} ({len(merged)} rows total)")

for key, df_list in dfs.items():
    save_category(key, df_list)

print(f"\n🎯 Merge batch {batch_number} selesai ✅")