import praw
from datetime import datetime, timezone, timedelta
import re
import time

# Initialize Reddit API connection
reddit = praw.Reddit(
    client_id="gR3zDGhX8kdahLKPD9nR7A",
    client_secret="k1Ts6pX3jzxYsVTYE4nZsnPCvmcAfw",
    user_agent="wsb-crawler by u/Over-Bar5374"
)

subreddit = reddit.subreddit("wallstreetbets")

# Last 30 days timestamps
end_time = datetime.now(tz=timezone.utc)
start_time = end_time - timedelta(days=30)
start_timestamp = int(start_time.timestamp())
end_timestamp = int(end_time.timestamp())

# Function to extract tickers
def extract_tickers(text):
    pattern = r'\$?[A-Z]{2,5}'   # match optional $ + 2-5 capital letters
    tickers = re.findall(pattern, text)
    blacklist = {"WSB", "DD", "YOLO", "ETF", "LOL"}
    return [t.replace("$", "") for t in tickers if t not in blacklist]

# Fetch posts from last 30 days using subreddit.new() and filter by timestamp
for post in subreddit.new(limit=1000):  # adjust limit as needed
    if start_timestamp <= post.created_utc <= end_timestamp:
        tickers = extract_tickers(post.title + " " + post.selftext)
        print(f"ID: {post.id}")
        print(f"Date: {datetime.fromtimestamp(post.created_utc)}")
        print(f"Title: {post.title}")
        print(f"Score: {post.score}")
        print(f"Comments: {post.num_comments}")
        print(f"Tickers: {tickers}")
        print("-" * 50)
