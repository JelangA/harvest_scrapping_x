import tweepy

# Inisialisasi autentikasi menggunakan Bearer Token
client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAACxk5AEAAAAAifqxwn00npxdiF%2F3mmgI7%2B1UNy0%3D8A7FOB4N0RUIKJcpxowsgE7k1lWkhsQ1fU8CpxOxj062gcTKi5')

# Request data Tweet dengan parameter field tertentu
response = client.get_tweet(
    id="1212092628029698048",
    tweet_fields=["attachments", "author_id", "context_annotations", "created_at", "entities", "geo", "id", "in_reply_to_user_id", "lang", "possibly_sensitive", "public_metrics", "referenced_tweets", "text", "withheld"],
    expansions=["referenced_tweets.id"]
)

# Menampilkan hasil
print("Status:", response.errors or "200 OK")
print("Tweet Data:", response.data)
print("Public Metrics:", response.data.public_metrics)
