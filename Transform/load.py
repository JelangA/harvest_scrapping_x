import os
import pandas as pd
from pymongo import MongoClient


# ====== PATH FOLDER HASIL ======
RESULT_DIR = "./result"

# ====== FUNGSI KONEKSI ======
def get_connection():
    uri = f"mongodb://{USERNAME}:{PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(uri)
    print("✅ Connected to MongoDB!")
    return client


# ====== UPLOAD CSV KE MONGO ======
def upload_csv_to_mongo(filename: str, collection_name: str):
    client = get_connection()

    # gunakan database default "test"
    db = client["test"]
    collection = db[collection_name]

    csv_path = os.path.join(RESULT_DIR, filename)
    print(f"📄 Membaca file: {csv_path}")

    df = pd.read_csv(csv_path)
    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)
        print(f"✅ Berhasil memasukkan {len(records)} data ke koleksi '{collection_name}'")
    else:
        print(f"⚠️ Tidak ada data di {filename}")


def main():
    print("🚀 Memulai upload CSV ke MongoDB...\n")

    files = {
        "ekonomi.csv": "ekonomi",
        "politik.csv": "politik",
        "sosial.csv": "sosial"
    }

    for file_name, collection_name in files.items():
        file_path = os.path.join(RESULT_DIR, file_name)
        if os.path.exists(file_path):
            upload_csv_to_mongo(file_name, collection_name)
        else:
            print(f"⚠️ File {file_name} tidak ditemukan — dilewati.")

    print("\n🏁 Selesai mengunggah semua file!")


if __name__ == "__main__":
    main()
