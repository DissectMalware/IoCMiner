import tweepy
import json
import datetime

def get_twitter_api():
    with open(r'config/tweeter.auth', 'r') as auth_file:
        consumer_key, consumer_secret, access_token, access_token_secret = auth_file.read().split()

        # OAuth process, using the keys and tokens
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=True)

        return api


def get_user_timeline(api, screen_name, count = 200):
    # Twitter only allows access to a users most recent 3240 tweets with this method
    # initialize a list to hold all the tweepy Tweets

    alltweets = []

    fetched_count = 200

    # make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name=screen_name, count=200)

    # save most recent tweets
    alltweets.extend(new_tweets)

    print("...%s tweets downloaded so far" % (len(alltweets)))

    if fetched_count >= count:
        return alltweets[:count]

    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        # all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)


        # save most recent tweets
        alltweets.extend(new_tweets)

        print("...%s tweets downloaded so far" % (len(alltweets)))

        if len(alltweets) >= count:
            break

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1




    return alltweets[:count]


def get_list_timeline(api, list_id, count):
    # Twitter only allows access to a users most recent 3240 tweets with this method
    # initialize a list to hold all the tweepy Tweets

    alltweets = []

    fetched_count = 200
    # make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.list_timeline(list_id=list_id, count=200)

    # save most recent tweets
    alltweets.extend(new_tweets)

    print("...%s list downloaded so far" % (len(alltweets)))
    if fetched_count >= count:
        return alltweets[:count]

    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        # all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(list_id=list_id, count=200, max_id=oldest)

        fetched_count += 200

        # save most recent tweets
        alltweets.extend(new_tweets)

        print("...%s list downloaded so far" % (len(alltweets)))

        if fetched_count >= count:
            break

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1



    return alltweets[:count]

def get_date():
    return datetime.datetime.now().strftime('%Y%m%d')
