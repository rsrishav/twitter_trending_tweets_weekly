import os
import time
import tweepy
import shutil
import pandas as pd
from constants import *
from datetime import datetime
from kaggle import KaggleApi as kag_api

DATASET_NAME = "twitter-trending-tweets"
DATA_FOLDER = "datasets"

CURRENT_DATETIME = datetime.utcnow()
TWEET_FILE_PATH = f"{DATA_FOLDER}/{CURRENT_DATETIME.year}_{CURRENT_DATETIME.strftime('%B')}_twitter_trending_data.csv"
ALL_TREND_FILE_PATH = f"{DATA_FOLDER}/{CURRENT_DATETIME.year}_{CURRENT_DATETIME.strftime('%B')}_all_trends_data.csv"
HASHTAG_TREND_FILE_PATH = f"{DATA_FOLDER}/{CURRENT_DATETIME.year}_{CURRENT_DATETIME.strftime('%B')}_hashtag_trend_data.csv"

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
    api = tweepy.API(auth, wait_on_rate_limit=True)
    print("[INFO] Twitter api authenticated.")
    return api


def kaggle_authenticate():
    api = kag_api()
    kag_api.authenticate(api)
    print("\n[INFO] Kaggle api authenticated.")
    return api


def get_trends(api):
    top_10_trends = dict()
    all_trends_hashtags = dict()
    all_trends = dict()
    for key, value in WOEID_DICT.items():
        print(f"[INFO] Fetching twitter trends for country: {value}")
        trends = api.trends_place(key)
        all_trends[value] = trends[0]["trends"]
        trend_hashtags = [trend for trend in trends[0]["trends"] if trend["name"][0] == "#"]
        all_trends_hashtags[value] = trend_hashtags
        top_10 = trend_hashtags[:10] if len(trend_hashtags) > 10 else trend_hashtags
        top_10_trends[value] = top_10
        print(f"> Trends fetched (max 10): {len(top_10)}\n")
        time.sleep(1)
    return top_10_trends, all_trends_hashtags, all_trends


def generate_tweet_data(api, country_wise_trends):
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


def generate_trends_data(country_trends):
    trend_data = list()
    for country, trends in country_trends.items():
        for trend in trends:
            d = [trend["name"], trend["url"], trend["query"], trend["tweet_volume"], CURRENT_DATETIME, country]
            trend_data.append(d)
    return trend_data


def convert_to_df(data, columns):
    df = pd.DataFrame(data, columns=columns, dtype=str)
    # print(df)
    return df


def save_df_csv(df, path):
    df.to_csv(path, index=False, mode='a', header=not os.path.exists(path))


def kaggle_dataset_download(api, dataset_name, path):
    kag_api.dataset_download_files(api, dataset_name, unzip=True, path=path)
    print("[INFO] Dataset downloaded.")


def kaggle_upload_dataset(api, path):
    kag_api.dataset_create_version(api, path, f"Dataset updated till (UTC): {datetime.utcnow()}",
                                   convert_to_csv=True, delete_old_versions=False)
    print("\n[INFO] Dataset uploaded.\n")
    clear_dir(path)


if __name__ == '__main__':
    t_api = twitter_authenticate()
    top_10, hashtag_trends, all_trends = get_trends(t_api)

    all_trends_data = generate_trends_data(all_trends)
    hashtag_trends_data = generate_trends_data(hashtag_trends)
    all_trends_df = convert_to_df(all_trends_data, TREND_COLUMNS)
    hashtag_trends_df = convert_to_df(hashtag_trends_data, TREND_COLUMNS)

    tweet_data = generate_tweet_data(t_api, top_10)
    tweet_df = convert_to_df(tweet_data, TWEET_COLUMNS)

    k_api = kaggle_authenticate()
    kaggle_dataset_download(k_api, DATASET_NAME, DATA_FOLDER)
    save_df_csv(all_trends_df, ALL_TREND_FILE_PATH)
    save_df_csv(hashtag_trends_df, HASHTAG_TREND_FILE_PATH)
    save_df_csv(tweet_df, TWEET_FILE_PATH)
    kaggle_upload_dataset(k_api, DATA_FOLDER)
