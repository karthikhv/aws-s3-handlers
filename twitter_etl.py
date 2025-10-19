import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs # Still included, but the S3 call is commented out

# Replace with your actual Bearer Token
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJyd4wEAAAAASRcz520d5mYOuLA7%2BE3jaxhhjDw%3DyCtHxS4eZLeYjjtDSisUrqhs4KY5FcpmLtkheMHrQ3JgDDNHdo" 

# Instantiate the V2 CLIENT
client = tweepy.Client(bearer_token=BEARER_TOKEN)


def run_twitter_etl():
    # 1. Look up the user ID for the screen name 'imVkohli'
    try:
        user_response = client.get_user(username='imVkohli')
        
        if not user_response.data:
            print("Error: Could not find user 'imVkohli'.")
            return

        user_id = user_response.data.id
        print(f"Successfully retrieved user ID for imVkohli: {user_id}")
        
    except (tweepy.errors.Unauthorized, tweepy.errors.Forbidden) as e:
        print(f"\nAuthentication/Permission Error: {e}")
        return

    # 2. Retrieve the user's recent tweets
    try:
        tweets_response = client.get_users_tweets(
            id=user_id,
            max_results=30,
            exclude=['retweets', 'replies'],
            tweet_fields=['created_at', 'public_metrics', 'author_id', 'text']
        )
        
        if tweets_response.data:
            print(f"Retrieved {len(tweets_response.data)} tweets.")
            
            # Convert tweets to a list of dictionaries for DataFrame creation
            tweet_list = []
            for tweet in tweets_response.data:
                tweet_list.append({
                    'id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'retweet_count': tweet.public_metrics.get('retweet_count', 0),
                    'like_count': tweet.public_metrics.get('like_count', 0)
                })

            df = pd.DataFrame(tweet_list)
            
            # ---------------------------------------------------------------------
            # CORE CHANGE: DISPLAY DATA IN TERMINAL
            # ---------------------------------------------------------------------
            print("\n" + "="*50)
            print("--- Extracted Data (First 5 Rows) ---")
            print("="*50)
            print(df.head().to_markdown(index=False)) # Use .to_markdown for clean terminal formatting
            
            print("\n" + "="*50)
            print("--- DataFrame Information ---")
            print("="*50)
            df.info() 
            # ---------------------------------------------------------------------
            
            
            # # 3. DATA LOADING (S3 Upload) - COMMENTED OUT
            # print("\nData is ready for S3 upload, skipping for terminal viewing...")
            # # current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            # # s3_file_key = f"twitter_data/imVkohli/tweets_{current_date}.csv"
            # # s3_path = f"s3://YOUR_S3_BUCKET_NAME/{s3_file_key}"
            # # try:
            # #     df.to_csv(s3_path, index=False)
            # #     print(f"Successfully uploaded {len(df)} tweets to S3 at: {s3_path}")
            # # except Exception as e:
            # #     print(f"\nS3 Upload Failed: {e}")
            
        else:
            print("No tweets found or data field is empty.")

    except tweepy.errors.Forbidden as e:
        print(f"\nForbidden Error: {e}")
        print("This action is restricted by your API Access Level (Error 453).")

if __name__ == "__main__":
    run_twitter_etl()