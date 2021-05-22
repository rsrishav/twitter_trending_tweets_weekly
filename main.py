import os
import time
import tweepy
import shutil
import pandas as pd
from constants import *
from datetime import datetime
from kaggle import KaggleApi as kag_api
# import twitter
# import geocoder

DATASET_NAME = "twitter-trending-tweets"
DATA_FOLDER = "datasets"

CURRENT_DATETIME = datetime.utcnow()
TWEET_FILE_PATH = f"{DATA_FOLDER}/{CURRENT_DATETIME.year}_{CURRENT_DATETIME.strftime('%B')}_twitter_trending_data.csv"

CONSUMER_KEY = os.environ["CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["CONSUMER_SECRET"]
OAUTH_TOKEN = os.environ["OAUTH_TOKEN"]
OAUTH_TOKEN_SECRET = os.environ["OAUTH_TOKEN_SECRET"]


def clear_dir(folder):
    for filename in os.listdir(folder):
        if filename == 'dataset-metadata.json':
            pass
        else:
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                print("[INFO] Files removed.")
            except Exception as e:
                print('[ERROR] Failed to delete %s. Reason: %s' % (file_path, e))


def twitter_authenticate():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # auth2 = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
    #                             CONSUMER_KEY, CONSUMER_SECRET)
    # twitter_api = twitter.Twitter(auth=auth2)
    print("[INFO] Twitter api authenticated.")
    return api


def kaggle_authenticate():
    api = kag_api()
    kag_api.authenticate(api)
    print("[INFO] Kaggle api authenticated.")
    return api


def get_trends(api):
    country_wise_trends = dict()
    for key, value in WOEID_DICT.items():
        print(f"[INFO] Fetching twitter trends for country: {value}")
        trends = api.trends_place(key)
        trend_hashtags = [trend for trend in trends[0]["trends"] if trend["name"][0] == "#"]
        top_10 = trend_hashtags[:10] if len(trend_hashtags) > 10 else trend_hashtags
        country_wise_trends[value] = top_10
        print(f"> Trends fetched (max 10): {len(top_10)}\n")
        time.sleep(1)
    return country_wise_trends


def get_tweets(api, country_wise_trends):
    tweet_data = list()
    # country_wise_trends = get_trends(api)
    for country, trends in country_wise_trends.items():
        for trend in trends:
            print(f"[INFO] Fetching tweets in {country} for hashtag: {trend['name']}")
            tweets = api.search(q=trend["name"], count=100)
            print(f"> Tweets fetched: {len(tweets)}")
            for tweet in tweets:
                d = [tweet.id_str, tweet.created_at, ", ".join([h["text"] for h in tweet.entities['hashtags']]),
                     tweet.lang, tweet.retweet_count, tweet.source, tweet.source_url, tweet.text, tweet.user.created_at,
                     tweet.user.name, tweet.user.followers_count, tweet.user.description, tweet.user.location,
                     trend["name"], CURRENT_DATETIME, country]
                tweet_data.append(d)
    return tweet_data


def convert_to_df(data):
    df = pd.DataFrame(data, columns=TWEET_HEADERS, dtype=str)
    print(df)
    return df


def save_df_csv(df, path):
    df.to_csv(path, index=False, mode='a', header=not os.path.exists(path))


def kaggle_dataset_download(api, dataset_name, path):
    kag_api.dataset_download_files(api, dataset_name, unzip=True, path=path)
    print("[INFO] Dataset downloaded.")


def kaggle_upload_dataset(api, path):
    kag_api.dataset_create_version(api, path, f"Dataset updated till (UTC): {datetime.utcnow()}",
                                   convert_to_csv=True, delete_old_versions=False)
    print("[INFO] Dataset uploaded.")
    clear_dir(path)


if __name__ == '__main__':
    t_api = twitter_authenticate()
    country_wise_trends = get_trends(t_api)
    tweet_data = get_tweets(t_api, country_wise_trends)
    df = convert_to_df(tweet_data)
    k_api = kaggle_authenticate()
    kaggle_dataset_download(k_api, DATASET_NAME, DATA_FOLDER)
    save_df_csv(df, TWEET_FILE_PATH)
    kaggle_upload_dataset(k_api, DATA_FOLDER)
