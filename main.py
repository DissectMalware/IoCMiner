import tweepy  # https://github.com/tweepy/tweepy
from queue import Queue
from threading import Thread
from gglsbl import SafeBrowsingList
import requests
import shutil
from CTI_expert_finder import *
from CTI_classifer import *
import numpy


class IOCMinerStreamListener(tweepy.StreamListener):

    def __init__(self, api, top_users, rand_users):
        self.api = api
        self.output = r"results\top_user_dump.json"
        self.output_file = open(self.output, "a")
        self.rand_users = rand_users
        self.top_users = top_users
        self.classifiers, self.wordlist = construct_classifier()
        self.enclosure_queue = Queue()
        self.worker = Thread(target=self.worker, args=(1, self.enclosure_queue,))
        self.worker.setDaemon(True)
        self.worker.start()


    def on_status(self, status):
        self.output_file.write(json.dumps(status._json )+'\n')
        self.enclosure_queue.put(status)

    def worker(self, id, queue):

        with open(r'config\gglsbl.auth', 'r') as auth_file:
            gglsbl_key = auth_file.read().strip()

        sbl = SafeBrowsingList(gglsbl_key, db_path=r"dataset\google_safe_browisng_db")
        # sbl.update_hash_prefix_cache()

        turn = True
        while True:

            # Update Google SBL database every 12 hours at time X (e.g. 3 AM and 3 PM)
            hour = datetime.datetime.today().hour
            if hour % 12 == 3 and turn:
                sbl.update_hash_prefix_cache()
                turn = False
            elif hour % 12 != 3:
                turn = True

            today = get_date()
            with open(os.path.join('results', today+'.ioc.csv'),'a+',encoding='utf_8') as output_file:
                tweet = queue.get()
                try:
                    if hasattr(tweet, 'retweeted_status') and hasattr(tweet.retweeted_status, 'extended_tweet') and 'full_text' in tweet.retweeted_status.extended_tweet:
                        text = tweet.retweeted_status.extended_tweet['full_text']
                    elif hasattr(tweet, 'extended_tweet') and 'full_text' in tweet.extended_tweet:
                        text = tweet.extended_tweet['full_text']
                    elif not hasattr(tweet, 'text'):
                        text = tweet['text']
                    else:
                        text = tweet.text

                    if hasattr(tweet, 'retweeted_status'):
                        if hasattr(tweet.retweeted_status, 'extended_tweet'):
                            final_urls = tweet.retweeted_status.extended_tweet['entities']['urls']
                        else:
                            final_urls = tweet.retweeted_status.entities['urls']
                    else:
                        if hasattr(tweet, 'extended_tweet'):
                            final_urls = tweet.extended_tweet['entities']['urls']
                        else:
                            final_urls = tweet.entities['urls']

                    for final_url in final_urls:
                        # If a pastebin URL, get the raw content and append it to the tweet content
                        if final_url['expanded_url'].startswith('https://pastebin.com/'):
                            pastebin = final_url['expanded_url']
                            if 'raw' not in pastebin:
                                pastebin = pastebin.replace('https://pastebin.com/', 'https://pastebin.com/raw/')

                            req = requests.get(pastebin)
                            text += '\n' + req.content

                    user_type = 'top'
                    if tweet.user.id_str in self.rand_users:
                        user_type = 'rand'

                    print("###########################$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                    print(text)

                    # classifier must be retrained with new data
                    # vector = vectorize(text, self.wordlist)
                    # vector.append(len(tweet.entities['hashtags']))
                    # vector.append(len(tweet.entities['user_mentions']))
                    # vector = numpy.array(vector).reshape(1, -1)
                    # estimates = []
                    # for i in range(number_of_classifiers):
                    #     y_estimate = self.classifiers[i].predict(vector)
                    #     estimates.append(y_estimate)
                    # vote = statistics.mode([x[0] for x in estimates])
                    # print("Prediction: "+vote)

                    ips = list(iocextract.extract_ips(text, refang=True))
                    for ip in ips:
                        if ip not in text:
                            output_file.write('{},{},{},{},{},ip,{}\n'.format(tweet.id,tweet.created_at, user_type, tweet.user.id_str, tweet.user.screen_name, ip))

                    urls = list(iocextract.extract_urls(text, refang=True))
                    for url in urls:
                        if url not in text:
                            result = sbl.lookup_url(url.rstrip('.'))
                            if result is not None:
                                output_file.write('{},{},{},{},{},url,{},{}\n'.format(tweet.id, tweet.created_at, user_type, tweet.user.id_str, tweet.user.screen_name, url.rstrip('.'),result))
                            else:
                                output_file.write('{},{},{},{},{},url,{},benign\n'.format(tweet.id, tweet.created_at, user_type, tweet.user.id_str, tweet.user.screen_name, url.rstrip('.')))

                    emails = list(iocextract.extract_emails(text, refang=True))
                    for email in emails:
                        if email not in text:
                            output_file.write('{},{},{},{},{},email,{}\n'.format(tweet.id, tweet.created_at, user_type, tweet.user.id_str, tweet.user.screen_name, email))
                    hashes = list(iocextract.extract_hashes(text))
                    for hash in hashes:
                        output_file.write('{},{},{},{},{},hash,{}\n'.format(tweet.id, tweet.created_at, user_type, tweet.user.id_str, tweet.user.screen_name, hash))
                except Exception as exp:
                    print(exp)

                queue.task_done()

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def __del__(self):
        self.output_file.close()


UPDATE_CURRENT_USER = False

api = get_twitter_api()
val = api.rate_limit_status()

if api.verify_credentials():

    today = get_date()

    base_dir = os.path.join(r'results\days', today)

    top_users_final_path = os.path.join(base_dir, 'top_users_final')
    if not os.path.exists('top_users_final'):
        top_users = dump_cti_experts(api, base_dir, test_run=True)
        if UPDATE_CURRENT_USER:
            shutil.copy2('results\\current_users.txt', 'results\\current_users.old.txt')
            with open('results\\current_users.txt','w', encoding='utf_8') as current_users_file:
                for user in top_users:
                    current_users_file.write("{},{}\n".format( user[0], user[1]))

    user_ids = []
    with open(os.path.join(base_dir, 'top_users_final'), 'r', encoding='utf_8') as top_user_file:
        next(top_user_file)
        csv_reader = csv.reader(top_user_file)
        for row in csv_reader:
            user_ids.append(row[0])

    rand_user_ids = []
    # with open(os.path.join(base_dir, 'rand_users_final_1k'), 'r', encoding='utf_8') as top_user_file:
    #     next(top_user_file)
    #     csv_reader = csv.reader(top_user_file)
    #     for row in csv_reader:
    #         rand_user_ids.append(row[0])

    twitter_listener = IOCMinerStreamListener(api, set(user_ids), set(rand_user_ids))
    IOC_stream = tweepy.Stream(auth=api.auth, listener=twitter_listener)

    # Collect tweets from a set of top CTI experts and a set of randomly selected users (indefinite loop)
    while True:
        try:
            users = user_ids[0:1000]
            users.extend(rand_user_ids)
            IOC_stream.filter(follow=users)
        except Exception as exp:
            print(str(exp))
            IOC_stream.disconnect()

        # If the stream listener is terminated, wait 120 seconds before creating a new one
        time.sleep(120)

