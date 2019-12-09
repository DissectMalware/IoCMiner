import os
import glob
import csv
import time
import re
import math
from utility import *
import iocextract
from dateutil.parser import parse

class Dummy(object):
    pass

def get_user_lists(api, user, max_count=1000):
    res = api.lists_memberships(screen_name=user, count=max_count)
    # Sorting the results based on subscriber_count and member_count
    res = sorted([i for i in res if i.mode.lower() == 'public'], key=lambda x: x.member_count, reverse=True)
    res = sorted([i for i in res], key=lambda x: x.subscriber_count, reverse=True)
    return res


def dump_user_lists(user, lists, dump_file_path):
    with open(dump_file_path, 'w', encoding='utf_8', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        row = [
            'id',
            'name',
            'slug',
            'description',
            'member_count',
            'subscriber_count',
            'mode',
            'created_at',
            'owner.id_str',
            'owner.screen_name',
            'owner.name',
            'owner.favourites_count',
            'owner.followers_count',
            'owner.friends_count',
            'owner.created_at'
        ]
        csv_writer.writerow(row)

        for list in lists:
            row = [
                list.id,
                list.name,
                list.slug,
                list.description,
                list.member_count,
                list.subscriber_count,
                list.mode,
                list.created_at,
                list.user.id_str,
                list.user.screen_name,
                list.user.name,
                list.user.favourites_count,
                list.user.followers_count,
                list.user.friends_count,
                list.user.created_at,
            ]
            csv_writer.writerow(row)


def dump_list_tweets(list, tweets, dump_file_path):
    with open(dump_file_path, 'w', encoding='utf_8') as output_file:
        for tweet in tweets:
            json_str = json.dumps(tweet._json)
            output_file.write(json_str + '\n')


def dump_list_users(list, users, dump_file_path):
    with open(dump_file_path, 'w', encoding='utf_8') as output_file:
        for user in users:
            json_str = json.dumps(user._json)
            output_file.write(json_str + '\n')


def get_list_members(api, list_id, max_count=5000):
    res = api.list_members(list_id=list_id, count=max_count)
    return res


def create_list(api, name):
    res = api.create_list(name, mode='private')
    return res.id_str


def add_to_list(api, list_id, members):
    for i in range(0, len(members), 100):
        api.add_list_members(list_id=list_id, user_id=members[i: i + 100])


def get_current_user():
    result = []
    current_users = r'results\current_users.txt'
    with open(current_users, 'r', encoding='utf_8') as input:
        for line in input:
            user, id = line.strip().split(',')
            result.append((user, id))
    return result


def select_top_lists(all_lists,
                     avg_sec_word_count,
                     avg_member_score,
                     avg_subscriber_count,
                     avg_owner_strenght,
                     count=1000):
    for i in all_lists:
        all_lists[i]['score'] = (all_lists[i]['sec_word_count'] / avg_sec_word_count) * \
                                (all_lists[i]['member_score'] / avg_member_score) * \
                                (all_lists[i]['subscriber_count'] / avg_subscriber_count) * \
                                (all_lists[i]['owner_strength'] / avg_owner_strenght)
    lists_rank = sorted(all_lists.items(), key=lambda x: x[1]['score'], reverse=True)
    discard_index = len(all_lists)
    counter = 0
    for i in lists_rank:
        if i[1]['score'] == 0:
            discard_index = counter
            break
        counter += 1

    if discard_index > count:
        discard_index = count

    lists_rank = lists_rank[:discard_index]

    return lists_rank


def dump_cti_experts(api, base_dir, test_run=False):
    # If you want to
    top_users_res = []
    user_dir = os.path.join(base_dir, 'users')
    user_status_dir = os.path.join(base_dir, r'users\status')
    list_dir = os.path.join(base_dir, 'lists')

    if not os.path.exists(user_status_dir):
        os.makedirs(user_status_dir)
    if not os.path.exists(list_dir):
        os.makedirs(list_dir)

    # For each CTI expert (in the input list), dump the info of all the lists that the expert is a member into a file
    for user, user_id in get_current_user():
        try:
            user_lists_dump_path = os.path.join(user_dir, user_id + '.user.csv')
            if not os.path.exists(user_lists_dump_path):
                lists = get_user_lists(api, user)
                dump_user_lists(user, lists, user_lists_dump_path)
                time.sleep(2)

            if test_run:
                break

        except Exception as exp:
            print('ERROR {}:{}'.format(user, exp.reason))

    list_rank = []
    specific_words = ['ioc',
                      'malware',
                      'Indicator.?of.?Compromise',
                      'threat.?hunt',
                      'threat.?hunt',
                      'phishing.?hunt',
                      'phish.?hunt',
                      'threat.?int',
                      'threat.?research',
                      'ransomware',
                      'mal.?doc']

    generic_words = ['info.?sec',
                     'cyber.?sec',
                     'security',
                     'ransomware']

    specific_regex_rule = re.compile('|'.join(specific_words), re.IGNORECASE)
    generic_regex_rule = re.compile('|'.join(generic_words), re.IGNORECASE)

    # sub_scores: number of relevant words, number_follower/log(number_followers), number_subscriber, owner_strength
    # score is a product of the above sub scores
    # each sub score must be in the range [0,+infinity), however, average must be 1
    # sub scores that are above average increase the total score

    all_lists = {}
    total_sec_word_count = 0
    total_member_score = 0
    total_subscriber_count = 0
    total_owner_strength = 0


    for file in glob.glob(os.path.join(user_dir, "*.user.csv")):
        with open(file, 'r', encoding='utf_8') as input_file:
            reader = csv.reader(input_file)
            next(reader)
            counter = 0
            for row in reader:
                counter += 1
                id = row[0]
                if id not in all_lists:
                    all_lists[id] = {}
                    all_lists[id]['id'] = row[0]
                    all_lists[id]['name'] = row[1]
                    all_lists[id]['text'] = row[1] + ' ' + row[3]
                    all_lists[id]['sec_word_count'] = len(specific_regex_rule.findall(all_lists[id]['text'])) * 3 + \
                                                      len(generic_regex_rule.findall(all_lists[id]['text']))
                    total_sec_word_count += all_lists[id]['sec_word_count']

                    all_lists[id]['member_count'] = int(row[4])
                    if all_lists[id]['member_count'] > 1:
                        all_lists[id]['member_score'] = all_lists[id]['member_count'] / math.log2(
                            all_lists[id]['member_count'])
                    else:
                        all_lists[id]['member_score'] = 0
                    total_member_score += all_lists[id]['member_score']

                    all_lists[id]['subscriber_count'] = int(row[5])
                    all_lists[id]['subscriber_count'] += 1
                    total_subscriber_count += all_lists[id]['subscriber_count']

                    all_lists[id]['owner_screen_name'] = row[9]
                    all_lists[id]['owner_followers_count'] = int(row[12])
                    all_lists[id]['owner_friends_count'] = int(row[13])
                    if all_lists[id]['owner_friends_count'] >= 1:
                        all_lists[id]['owner_strength'] = math.log2(
                            (all_lists[id]['owner_followers_count'] + all_lists[id]['owner_friends_count']) /
                            all_lists[id]['owner_friends_count'])
                    else:
                        all_lists[id]['owner_strength'] = 0
                    total_owner_strength += all_lists[id]['owner_strength']

                if test_run:
                    if counter > 10:
                        break

    avg_sec_word_count = total_sec_word_count / len(all_lists)
    avg_member_score = total_member_score / len(all_lists)
    avg_subscriber_count = total_subscriber_count / len(all_lists)
    avg_owner_strength = total_owner_strength / len(all_lists)

    top_lists = select_top_lists(all_lists,
                                 avg_sec_word_count,
                                 avg_member_score,
                                 avg_subscriber_count,
                                 avg_owner_strength)

    counter = 0

    # Dump the latest 1000 timeline tweets of each top lists
    for top_list in top_lists:
        try:
            print(top_list[0] + '\t' + top_list[1]['owner_screen_name'] + '\t\t' + top_list[1]['name'])
            file_name = top_list[0] + '---' + top_list[1]['owner_screen_name'] + '---' + top_list[1][
                'name'].replace('/', '-') + '.dump.list.csv'
            list_tweets_file_path = os.path.join(list_dir, file_name)
            if not os.path.exists(list_tweets_file_path):
                tweets = get_list_timeline(api, top_list[0], 1000)
                dump_list_tweets(top_list[0], tweets, list_tweets_file_path)
                counter += 1

            if test_run:
                if counter > 10:
                    break
            else:
                if counter > 150:
                    break
        except Exception as exp:
            print('ERROR processing tweets of ' + str(top_list[0]))
            print(exp)

    # For each List, count the number of IoCs appread in the dump of the latest 1000 timeline tweets
    top_lists_iocs = {}
    ioc_global_freq = {}
    count = 0
    for file in glob.glob(os.path.join(list_dir, "*.dump.list.csv")):
        count += 1
        name = os.path.basename(file)
        print('processing ' + name)
        id = name.split('---')[0]
        if id not in top_lists_iocs:
            top_lists_iocs[id] = set()
        with open(file, 'r', encoding='utf_8') as input_file:
            for line in input_file:
                try:
                    tweet = json.loads(line)
                    iocs = iocextract.extract_iocs(tweet['text'], refang=True)
                    for ioc in iocs:
                        if ioc not in tweet['text']:
                            top_lists_iocs[id].add(ioc)
                            if ioc not in ioc_global_freq:
                                ioc_global_freq[ioc] = 1
                            else:
                                ioc_global_freq[ioc] += 1
                except Exception as exp:
                    print('ERROR processing ' + name + ' tweet: ' + line)

    # Calculate the uniqueness score for each of the lists
    list_ranking = {}
    average_score = 0
    for list_id, iocs in top_lists_iocs.items():
        total_score = 0
        for ioc in iocs:
            ioc_count = ioc_global_freq[ioc] + 1
            # total_score += 1 / math.log2(ioc_count)
            total_score += 1 / ioc_count
        list_ranking[list_id] = total_score

        average_score += total_score

    average_score = average_score / len(list_ranking)

    list_rank_ioc = []
    for top_list in top_lists:
        if top_list[0] in list_ranking:
            top_list[1]['ioc_uniqness'] = list_ranking[top_list[0]] / average_score
            top_list[1]['score'] *= top_list[1]['ioc_uniqness']
            list_rank_ioc.append(top_list[1])

    ranked_list = sorted(list_rank_ioc, key=lambda x: x['score'], reverse=True)

    with open(os.path.join(list_dir, 'list_ioc_rank'), 'w', encoding='utf_8') as rank_output:
        for list in ranked_list:
            rank_output.write(
                '{},{},{},{}\n'.format(list['id'], list['owner_screen_name'], list['name'], list['score']))

    member_scores = {}
    for list in ranked_list:
        try:

            file_name = list['id'] + '---' + list['owner_screen_name'] + '---' + list['name'].replace('/',
                                                                                                      '-') + '.members.list.csv'
            print('Getting members of ' + list['id'])
            list_members_file_path = os.path.join(list_dir, file_name)
            if not os.path.exists(list_members_file_path):
                members = get_list_members(api, list['id'])
                print('List members count ' + str(len(members)))
                dump_list_users(list['id'], members, list_members_file_path)
            else:
                members =[]
                with open (list_members_file_path, 'r') as member_file:
                    for line in member_file:
                        member = Dummy()
                        member_json_obj = json.loads(line)
                        member.screen_name = member_json_obj['screen_name']
                        member.id = member_json_obj['id']
                        members.append(member)

            for member in members:
                if member.id not in member_scores:
                    member_scores[member.id] = {'score': 0, 'screen_name': member.screen_name, 'lists': set()}

                member_scores[member.id]['lists'].add(list['id'])
                member_scores[member.id]['score'] += list['score']
            print('All members count ' + str(len(member_scores)))
        except Exception as exp:
            print('ERROR getting members ' + list_id)

    member_ranks = sorted(member_scores.items(), key=lambda x: x[1]['score'], reverse=True)

    with open(os.path.join(base_dir, 'top_users'), 'w', encoding='utf_8', newline='') as top_users_output:
        writer = csv.writer(top_users_output)
        writer.writerow(['id', 'screen_name', 'score', 'lists'])
        for member in member_ranks:
            writer.writerow([member[0], member[1]['screen_name'], member[1]['score'], member[1]['lists']])


    member_ranks = member_ranks[:1000]

    print("Top 1k users before considering users' tweeting history")
    print(member_ranks)

    count = 0
    # For each user in top_users file
    with open(os.path.join(base_dir, 'top_users'), 'r', encoding='utf_8', newline='') as top_users_input:
        csv_reader = csv.reader(top_users_input)
        next(csv_reader)
        user_iocs = {}
        ignore = True
        for row in csv_reader:
            user_id, screen_name, score = row[0], row[1], float(row[2])
            try:
                print(str(count) + " - Getting tweets of "+screen_name)
                user_tweets_file_path = os.path.join(user_status_dir, '{}_{}_tweets.csv'.format(user_id, screen_name))
                time_now = datetime.datetime.now()
                if screen_name not in user_iocs:
                    user_iocs[screen_name] = {'id': user_id, 'screen_name': screen_name, 'score': score, 'days': {}}

                # If we have not the tweet history of the user, collect the latest of their 400 timeline tweets
                if not os.path.exists(user_tweets_file_path):
                    all_tweets = get_user_timeline(api, screen_name, 400)
                    # write the csv
                    with open(os.path.join(user_status_dir, '{}_{}_tweets.csv'.format(user_id, screen_name)), 'w',
                              encoding='utf_8') as output_file:
                        # dump tweets
                        for i in all_tweets:
                            output_file.write(json.dumps(i._json) + '\n')
                            output_file.flush()
                else:
                    ignore = False
                    all_tweets = []
                    with open(user_tweets_file_path, 'r', encoding='utf_8') as input_file:
                        next(input_file)
                        for line in input_file:
                            try:
                                all_tweets.append(json.loads(line))
                            except Exception as exp:
                                print("Error loading tweets in "+ user_tweets_file_path)


                for tweet in all_tweets:
                    if not hasattr(tweet, 'text'):
                        text = tweet['text']
                    else:
                        text = tweet.text

                    if not hasattr(tweet, 'created_at'):
                        created_at = parse(tweet['created_at'])
                        created_at = created_at.replace(tzinfo=None)
                    else:
                        created_at = tweet.created_at

                    iocs = iocextract.extract_iocs(text, refang=True)
                    for ioc in iocs:
                        if ioc not in text:
                            day_diff = (time_now - created_at).days
                            if day_diff < 0:
                                day_diff = 0
                            if day_diff not in user_iocs[screen_name]['days']:
                                user_iocs[screen_name]['days'][day_diff] = set()

                            user_iocs[screen_name]['days'][day_diff].add(ioc)

                count += 1
            except Exception as exp:
                print('Error getting statuses of '+ screen_name)

            if test_run:
                if count > 20:
                    break
            else:
                if count > 5000:
                    break
            if count % 50 == 0:

                print("\n\n\n\ncurrent number " + str(count)+'\n\n\n\n')

        avg_ioc_score = 0
        for screen_name, ioc in user_iocs.items():
            ioc_score = 0
            for day, iocs in ioc['days'].items():
                ioc_score += len(iocs) / ((int(day)+1)**(1/3))
            ioc_score += 1
            ioc['ioc_score'] = ioc_score
            avg_ioc_score += ioc_score

        avg_ioc_score = avg_ioc_score / len(user_iocs)

        for screen_name, ioc in user_iocs.items():
            ioc['ioc_score'] /= avg_ioc_score
            ioc['total_score'] = ioc['ioc_score']* ioc['score']

        final_user_rank = sorted(user_iocs.items(), key=lambda x:x[1]['total_score'], reverse=True)

        with open(os.path.join(base_dir, 'top_users_final'), 'w', encoding='utf_8', newline='') as top_users_output:
            writer = csv.writer(top_users_output)
            writer.writerow(['id', 'screen_name', 'score','ioc_score', 'final_score','days'])
            for screen_name, details in final_user_rank:
                writer.writerow([details['id'],
                                        details['screen_name'],
                                        details['score'],
                                        details['ioc_score'],
                                        details['total_score'],
                                        json.dumps({x:len(y) for x,y in details['days'].items()})])
                top_users_res.append((details['screen_name'],details['id']))

    return top_users_res

