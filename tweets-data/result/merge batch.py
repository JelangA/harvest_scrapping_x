import os
import pandas as pd

BASE_DIR = "."
OUTPUT_DIR = "merge"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# File kategori yang ingin diproses
TARGET_FILES = ["ekonomi.csv", "politik.csv", "sosial.csv"]

# Menyimpan list file per kategori
files_map = {name: [] for name in TARGET_FILES}

# Cari semua folder kecuali merge/
all_folders = [
    d for d in os.listdir(BASE_DIR)
    if os.path.isdir(os.path.join(BASE_DIR, d)) and d != OUTPUT_DIR
]

# Loop folder dan kumpulkan file
for folder in all_folders:
    folder_path = os.path.join(BASE_DIR, folder)

    for fname in os.listdir(folder_path):
        if fname in TARGET_FILES:
            files_map[fname].append(os.path.join(folder_path, fname))

# Merge & remove duplicates by `id`
for filename, paths in files_map.items():
    if not paths:
        print(f"Skip {filename} (tidak ditemukan)")
        continue

    # Gabungkan semua file
    df = pd.concat((pd.read_csv(p) for p in paths), ignore_index=True)

    # ✅ Hapus duplikasi berdasarkan kolom 'id'
    if "id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["id"], keep="first")
        after = len(df)
        print(f"{filename}: removed {before-after} duplicate rows based on id")
    else:
        print(f"{filename}: kolom 'id' tidak ditemukan, skip dedup")

    # Simpan file merge
    out_name = filename.replace(".csv", "_merge.csv")
    out_path = os.path.join(OUTPUT_DIR, out_name)

    df.to_csv(out_path, index=False)
    print(f"Created: {out_path}")

print("✅ Merge selesai & duplikasi berdasarkan id telah dibersihkan!")
