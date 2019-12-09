import os
import glob
import json

ioc_base_dir = r'results'

class TweetInfo:
    def __init__(self, tweet):
        self.tweet = tweet
        self.responses = []
        self.reply_to = None

    @staticmethod
    def get_all_text(tweet):
        result = ''
        for response in tweet.responses:
            result += response.tweet + '\n'
            result += TweetInfo.get_all_text(response) + '\n'
        return result


    def __str__(self):
        parent = self.reply_to
        ancestors_text = ''
        while parent != None:
            ancestors_text = parent.tweet +'\n' + ancestors_text

        return '[ancestors_text]\n'+ancestors_text+'\n[self]\n'+self.tweet +'\n[decendent]\n'+ TweetInfo.get_all_text(self)


def get_text(tweet):
    if 'retweeted_status' in tweet and \
            'extended_tweet' in tweet['retweeted_status'] and \
            'full_text' in tweet['retweeted_status']['extended_tweet']:
        text = tweet['retweeted_status']['extended_tweet']['full_text']
    elif 'extended_tweet' in tweet and 'full_text' in tweet['extended_tweet']:
        text = tweet['extended_tweet']['full_text']
    else:
        text = tweet['text']
    return text

def get_ioc_tweet_ids(file):
    result = set()
    iocs = set()
    with open(file, 'r', encoding='utf_8') as input_file:
        for line in input_file:
            segments = line.strip().split(',')
            seg_len = len(segments)
            if seg_len >= 7:

                segments[6] = segments[6].strip()
                if segments[6] not in iocs and segments[5] != 'email':
                    result.add(segments[0])
                    iocs.add(segments[6])
    return result

if __name__ == "__main__":
    # Get the IoCs from the daily IoC files
    ioc_tweet_ids = set()
    for file in glob.glob(os.path.join(ioc_base_dir, '*.ioc.csv')):
        file_name = file.split('\\')[-1]
        res = get_ioc_tweet_ids(file)
        ioc_tweet_ids.update(res)

    tweets = {}
    with open(os.path.join(ioc_base_dir, 'top_user_dump.json'), 'r', encoding='utf_8') as input_file:
        tag_dataset = {}
        seen_tweets = set()
        # Construct tweet threads from a tweet dump
        for count, line in enumerate(input_file):
            tweet = json.loads(line)
            current_tweet = TweetInfo(get_text(tweet))
            if tweet['in_reply_to_status_id_str'] == None:
                tweets[tweet['id_str']] = current_tweet
            else:
                if tweet['in_reply_to_status_id_str'] in tweets:
                    tweets[tweet['in_reply_to_status_id_str']].responses.append(current_tweet)
                    tweets[tweet['id_str']] = current_tweet

        # Print the tweet threads containing IoCs
        for ioc_tweet_id in ioc_tweet_ids:
            if ioc_tweet_id in tweets:
                if len(tweets[ioc_tweet_id].responses) > 0:
                    print(tweets[ioc_tweet_id])



