import os
import pandas as pd

# 📂 Path absolut biar aman dari error relatif
RAPID_DIR = os.path.join("../Transform/rapid_data")
CRAWL_DIR = os.path.join("../Transform/crawling_data")
OUTPUT_DIR = os.path.join("../Transform/result")

os.makedirs(OUTPUT_DIR, exist_ok=True)


class TweetTransformer:
    """Universal transformer for both RapidAPI and Crawling data"""

    def __init__(self, path: str):
        self.path = path
        self.df = pd.read_csv(path)

    def transform(self) -> pd.DataFrame:
        cols = self.df.columns

        # Deteksi tipe file
        if "views_count" in cols:
            return self._transform_crawling()
        else:
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


def process_folder(folder: str) -> pd.DataFrame:
    """Transform semua file CSV di folder (jika folder ada)"""
    if not os.path.exists(folder):
        print(f"⚠️  Folder tidak ditemukan: {folder} — dilewati.")
        return pd.DataFrame()

    all_frames = []
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            path = os.path.join(folder, filename)
            print(f"🔄 Memproses: {path}")
            try:
                transformer = TweetTransformer(path)
                transformed_df = transformer.transform()
                all_frames.append(transformed_df)
            except Exception as e:
                print(f"❌ Gagal memproses {filename}: {e}")

    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


def main():
    print("🚀 Memulai transformasi data tweet...\n")

    rapid_df = process_folder(RAPID_DIR)
    crawl_df = process_folder(CRAWL_DIR)

    combined = pd.concat([rapid_df, crawl_df], ignore_index=True)
    output_path = os.path.join(OUTPUT_DIR, "combined_tweets.csv")

    combined.to_csv(output_path, index=False)
    print(f"\n✅ Transformasi selesai!")
    print(f"📁 File hasil: {output_path}")
    print(f"📊 Total baris: {len(combined)}")


if __name__ == "__main__":
    main()
