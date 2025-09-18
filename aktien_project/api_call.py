import praw

reddit = praw.Reddit(
    client_id="DEINE_CLIENT_ID",
    client_secret="DEIN_SECRET",
    user_agent="mein_crawler"
)

subreddit = reddit.subreddit("wallstreetbets")

for post in subreddit.top(time_filter="month", limit=100):
    print(post.title, post.score)
