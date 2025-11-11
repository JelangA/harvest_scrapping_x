import os
import pandas as pd

# 📂 Path folder input & output (gunakan absolut relatif dari posisi file ini)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAPID_DIR = os.path.join(BASE_DIR, "rapid_data")
CRAWL_DIR = os.path.join(BASE_DIR, "crawling_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "result")

os.makedirs(OUTPUT_DIR, exist_ok=True)


class TweetTransformer:
    """Universal transformer for both RapidAPI and Crawling data"""

    def __init__(self, path: str):
        self.path = path
        self.df = pd.read_csv(path)

    def transform(self) -> pd.DataFrame:
        cols = self.df.columns

        if "views_count" in cols:  # Crawling version
            return self._transform_crawling()
        else:  # RapidAPI version
            return self._transform_rapid()

    def _transform_rapid(self) -> pd.DataFrame:
        return pd.DataFrame({
            "tweet_id": self.df.get("id_str"),
            "user_screen_name": self.df.get("username"),
            "created_at": self.df.get("created_at"),
            "text": self.df.get("full_text"),
            "like_count": self.df.get("like_count"),
            "retweet_count": self.df.get("retweet_count"),
            "reply_count": self.df.get("reply_count"),
            "quote_count": self.df.get("quote_count"),
            "view_count": None,
        })

    def _transform_crawling(self) -> pd.DataFrame:
        return pd.DataFrame({
            "tweet_id": self.df.get("id_str"),
            "user_screen_name": None,
            "created_at": self.df.get("created_at"),
            "text": self.df.get("full_text"),
            "like_count": self.df.get("like_count"),
            "retweet_count": self.df.get("retweet_count"),
            "reply_count": self.df.get("reply_count"),
            "quote_count": self.df.get("quote_count"),
            "view_count": self.df.get("views_count"),
        })


def process_folder(folder: str) -> dict:
    """Transform semua file CSV di folder dan kembalikan hasil per kategori"""
    if not os.path.exists(folder):
        print(f"⚠️  Folder tidak ditemukan: {folder} — dilewati.")
        return {}

    result = {}
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            topic = (
                filename.replace("_merge", "")
                .replace(".csv", "")
                .strip().lower()
            )
            path = os.path.join(folder, filename)
            print(f"🔄 Memproses: {path}")

            try:
                transformer = TweetTransformer(path)
                df = transformer.transform()
                if topic not in result:
                    result[topic] = []
                result[topic].append(df)
            except Exception as e:
                print(f"❌ Gagal memproses {filename}: {e}")

    return result


def main():
    print("🚀 Memulai transformasi data tweet...\n")

    # Ambil hasil rapid dan crawling per topik
    rapid_result = process_folder(RAPID_DIR)
    crawl_result = process_folder(CRAWL_DIR)

    # Gabungkan hasil dari kedua sumber berdasarkan topik
    all_topics = set(rapid_result.keys()) | set(crawl_result.keys())
    for topic in all_topics:
        frames = []
        if topic in rapid_result:
            frames.extend(rapid_result[topic])
        if topic in crawl_result:
            frames.extend(crawl_result[topic])

        combined = pd.concat(frames, ignore_index=True)
        output_path = os.path.join(OUTPUT_DIR, f"{topic}.csv")

        combined.to_csv(output_path, index=False)
        print(f"✅ Selesai: {topic}.csv — {len(combined)} baris")

    print(f"\n📁 Semua hasil disimpan di folder: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
