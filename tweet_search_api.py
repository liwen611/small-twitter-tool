#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 23 09:33:09 2019

@author: liwenhuang
"""

import string
import time
import re
import tweepy
import json
import csv
import os

class TweetSearch():
    """The TweetSearch class perform the functions of searching the tweeter api for tweet containing certain key word, output and aggregate the search results"""
    def __init__(self):
        self.consumer_key = 'xxxxxxxxxxxxxxxxxxxxx'
        self.consumer_secret = 'xxxxxxxxxxxxxxxxxxxxx'

    def connect_api(self):
        """this function connects the api"""
        self.auth = tweepy.AppAuthHandler(self.consumer_key, self.consumer_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        if not self.api:
            print("Problem Connecting to API")

    def check_limit(self):
        """this function print out the remaining api"""
        print(self.api.rate_limit_status()['resources']['search'])

    def search(self, keyword='goop'):
        """the search function take in a simple UTF-8, URL-encoded search query of 500 characters maximum, output 500 search result to a json line file, the default keyword is set to 'goop'"""
        print("Dowdloaing 500 tweets ...")
        counter = 0
        with open('%s_%s.json' % (keyword + '_search_', time.strftime('%Y%m%d-%H%M%S')), 'w') as writer:
            while counter < 500:
                search_result = self.api.search(q=keyword, count=100)
                for i in range(len(search_result)):
                    raw_tweet = search_result[i]._json['text'].lower().translate(str.maketrans('', '', string.punctuation))
                    if keyword not in raw_tweet:
                        continue #filter result where 'goop' is not in the tweet text
                    if counter >= 500:
                        break
                    counter += 1
                    data = {}
                    data['time_stamp'] = search_result[i]._json['created_at']
                    data['tweet_id'] = search_result[i]._json['id']
                    data['text'] = search_result[i]._json['text']
                    data['user_id'] = search_result[i]._json['user']['id']
                    data['loacation'] = search_result[i]._json['user']['location']
                    data['follower_count'] = search_result[i]._json['user']['followers_count']
                    data['user_info'] = [search_result[i]._json['user']['name'], search_result[i]._json['user']['screen_name'], search_result[i]._json['user']['description']]
                    data['register_time'] = search_result[i]._json['user']['created_at']
                    data['retweeted'] = search_result[i]._json['retweeted']
                    json_record = json.dumps(data)
                    writer.write(json_record + '\n') #each line is a json object

    def processTweet(self,tweet):
        """this function takes a raw tweet string as input, did some basic processing and return the cleaned string"""
        tweet = tweet.lower()
    #Convert www.* or https?://* to URL
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','URL',tweet)
    #Convert @username to AT_USER
        tweet = re.sub('@[^\s]+','AT_USER',tweet)
    #Remove additional white spaces
        tweet = re.sub('[\s]+', ' ', tweet)
    #Replace #word with word
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
    #trim
        tweet = tweet.strip('\'"')
        return tweet

    def aggregate_json(self, keyword='goop'):
        """the aggregate_json function find all the search result json file containing certain keyword and aggregate them to a single csv file, the default keyword is set to 'goop'"""
        fieldnames =['tweet_text', 'time_stamp', 'user_location', 'followers_count']
        with open('%s_%s.csv' % (keyword + '_search_', time.strftime('%Y%m%d-%H%M%S')), 'w') as output_file:
            report =csv.writer(output_file)
            report.writerow(fieldnames)

            counter = 0
            last_tweet = ''
            for file in os.listdir():
                if file.startswith(keyword + '_search_') and file.endswith('.json'):
                    print(os.path.join(file) + " is proccessed")
                    with open(file) as input_file:
                        for line in input_file:
                            data = json.loads(line)
                            tweet_text = self.processTweet(data['text'])
                            time_stamp = data['time_stamp']
                            user_location = data['loacation']
                            followers_count = data['follower_count']
                            output_row = [tweet_text, time_stamp, user_location, followers_count]
                            counter += 1
                            #report.writerow(output_row)
        print("You have a total of " + str(counter) + " tweets in your output csv file.")
