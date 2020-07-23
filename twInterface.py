#!/usr/bin/env python

import sys, os
import argparse
import json
from datetime import datetime

from TwitterMySQL import TwitterMySQL as t
import TwitterMySQL.locationInfo as locationInfo
import TwitterMySQL.countryBoundingBoxes as cbb

import time
start = time.time()

DEFAULT_OUTPUTFILE = 'twOutput'
DEFAULT_USERFILE = ''
DEFAULT_MSGFILE = ''
DEFAULT_MONTHLY_TABLES = False
DEFAULT_DB = ''
DEFAULT_TABLE = 'msgs'
DEFAULT_AUTH = ''
DEFAULT_AUTH_JSON = ''
DEFAULT_BB = ''
DEFAULT_CBB = []
DEFAULT_SEARCH_TERMS = ''
DEFAULT_STREAM_TERMS = ''
DEFAULT_COLUMN_SH = ''
DEFAULT_RANDOM = False
DEFAULT_TL = False
DEFAULT_FU = False
DEFAULT_SN = False
DEFAULT_PP = False

def isScreeName(params_dict, id_str):
    if id_str.isdigit():
        params_dict['user_id'] = int(id_str)
        if 'screen_name' in params_dict:
            del params_dict['screen_name']
    else:
        params_dict['screen_name'] = id_str
        if 'user_id' in params_dict:
            del params_dict['user_id']
    return params_dict

def createOutput(outputName, append):
    if not os.path.exists(outputName + append):
        os.makedirs(outputName + append)
    return outputName + append

if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Pull tweets from Twitter API')
    parser.add_argument('-d', dest='db', default=DEFAULT_DB, help='MySQL database where tweets will be stored.')
    parser.add_argument('-t', dest='table', default=DEFAULT_TABLE, help='MySQL table name. If monthly tables then M_Y will be appended to end of this string. Default: %s' % (DEFAULT_TABLE))
    parser.add_argument('--auth', dest='authfile', default=DEFAULT_AUTH, help='Path to authentication file which contains (line separated) CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET.')
    parser.add_argument('--authJSON', dest='authfileJSON', default=DEFAULT_AUTH_JSON, help='Path to JSON authentication file which can contain multiple keys. These keys will be cycled when rate limited.')
    parser.add_argument('--monthly_tables', action='store_true', dest='monthlytables', default=DEFAULT_MONTHLY_TABLES,
                        help='Turn on writing to monthly tables')
    parser.add_argument('--output_file', dest='outputfile', default=DEFAULT_OUTPUTFILE,
                        help='File where output from --social_network and --profile_pictures will be written. Default: twOutput')

    # pulling methods
    parser.add_argument('--bounding_box', type=str, metavar='FIELD(S)', dest='bb', nargs='+', default=DEFAULT_BB,
                        help='Pull tweets from bounding box. Must specify 3 or 4 cooridnates, space separated: LAT LON RADIUS (in miles) or LON LAT LON LAT (First pair is from bottom left corner of box, second pair is from top right corner).')
    #parser.add_argument('--country_bounding_box', dest='cbb', default=DEFAULT_CBB, help='Two letter country code for bounding box style grabber')
    parser.add_argument('--country_bounding_box', type=str, metavar='FIELD(S)', dest='cbb', nargs='*', default=DEFAULT_CBB,
                        help='Two letter country code for bounding box style grabber')
    
    parser.add_argument('--time_lines', action='store_true', dest='timelines', default=DEFAULT_TL,
                        help='Pull timelines for given list of user names / screen names.')
    parser.add_argument('--tweets_since_date', dest='tweetssincedate', default='',
                        help='Pull tweets since this date. Format: %%Y-%%m-%%d. Use with --time_lines')
    parser.add_argument('--follow_users', action='store_true', dest='followusers', default=DEFAULT_FU,
                        help='Given a user list (--user_list) will CONTINUOUSLY loop over the list and pull all tweets since their most recent tweet saved in the MySQL db (-t). If no user list is given then pull most recent tweets for all users in a given table (-t).')
    parser.add_argument('--follow_users_with_cron', action='store_true', dest='followuserswithcron', default=DEFAULT_FU,
                        help='Given a user list (--user_list) will loop over the list and pull all tweets since their most recent tweet saved in the MySQL db (-t). If no user list is given then pull most recent tweets for all users in a given table (-t) in a single loop.')
    parser.add_argument('--stream_terms', type=str, metavar='FIELD(S)', dest='streamterms', nargs='*', default=DEFAULT_STREAM_TERMS,
                        help='Pull FUTURE tweets containing keywords. Must supply space separated list of search terms. Ex: taylorswift #1989 #arianagrande')
    parser.add_argument('--stream_term_file', dest='streamfile', default=None,
                        help='Optional file containing list of stream terms to use')
    parser.add_argument('--search_terms', type=str, metavar='FIELD(S)', dest='searchterms', nargs='*', default=DEFAULT_SEARCH_TERMS,
                        help='Pull PAST tweets containing keywords. Must supply space separated list of search terms. Ex: taylorswift #1989 #arianagrande')
    parser.add_argument('--random_stream', action='store_true', dest='randomstream', default=DEFAULT_RANDOM,
                       help='Grab data from the random stream.')
    parser.add_argument('--social_network', action='store_true', dest='socialnetwork', default=DEFAULT_SN,
                        help='Pull social network for given list of user names / screen names. Must specify path to file which contains list.')
    parser.add_argument('--profile_pictures', action='store_true', dest='profilepictures', default=DEFAULT_PP,
                        help='Pull profile pictures for given list of user names / screen names. Must specify path to file which contains list.')
    parser.add_argument('--user_list', dest='userlist', default=DEFAULT_USERFILE,
                        help='File containing list of user ids / screen names')
    parser.add_argument('--message_list', dest='messagelist', default=DEFAULT_MSGFILE,
                        help='File containing list of message ids')
    parser.add_argument('--check_spam', dest='checkspam', default=False, action='store_true',
                        help='Check each message for spam')
    parser.add_argument('--no_retweets', dest='noretweets', default=False, action='store_true',
                        help='Do NOT save retweets when using statuses/user_timeline (--pull_timelines and --follow_users)')
    parser.add_argument('--trim_user', dest='trimuser', default=False, action='store_true',
                        help='Do NOT return User object in tweet when using statuses/user_timeline (--pull_timelines and --follow_users)')
    parser.add_argument('--column_short_list', type=str, metavar='FIELD(S)', dest='columnshortlist', nargs='*', default=DEFAULT_COLUMN_SH,
                        help='List of MySQL columns to save, instead of full list given in TwitterMySQL')
    parser.add_argument('--save_json', dest='savejson', default='',
                        help='Location (directory path) to save raw tweets as json files.')
    args = parser.parse_args()
    
    # where are we writing everything? no database needed for profile pictures or social networks
    if not (args.profilepictures or args.socialnetwork ) and not args.db:
        print "You must specify a database"
        sys.exit(1)

    # begin
    params = {'read_default_group': 'client',
              'db': args.db,
              'table': args.table,
              }

    # can we authenticate? first try authfile, does it exist and is it properly formatted
    if args.authfile:
        if not os.path.isfile(args.authfile):
            print("Your authentication file %s does not exist. Use --auth path/to/file" % (args.authfile))
            sys.exit(1)
        else:
            keys = [line.rstrip('\n') for line in open(args.authfile)]
            if len(keys) < 4:
                print "Something is wrong with your authentication file"
                sys.exit(1)
            (CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET) = keys
            params["API_KEY"] = CONSUMER_KEY
            params["API_SECRET"] = CONSUMER_SECRET
            params["ACCESS_TOKEN"] = ACCESS_TOKEN
            params["ACCESS_SECRET"] = ACCESS_SECRET
    # next try authJSON, does it exist and is it properly formatted
    elif args.authfileJSON:
        if not os.path.isfile(args.authfileJSON):
            print("Your authentication file %s does not exist. Use --authJSON path/to/file" % (args.authfile))
            sys.exit(1)
        else:
            try:
                with open(args.authfileJSON) as data_file:
                    cred_defs = json.load(data_file)
                params["authFile"] = args.authfileJSON
            except:
                print "Something is wrong with your authentication JSON file"
                sys.exit(1)
    # no authentication possible, exit
    else:
        print("You must specify an authfile with either --auth or --authJSON")
        sys.exit(1)

    if args.checkspam:
        params['checkSpam'] = True

    if args.savejson:
        if not os.path.isdir(args.savejson):
            print("The directory {data_dir} does not exist. Please create and rerun".format(data_dir=kwargs["saveJSON"]))
            sys.exit(1)
        params['saveJSON'] = args.savejson

    if args.bb:
        params['geoLocate'] = locationInfo.LocationMap().reverseGeocodeLocal
    elif args.cbb:
        params['geoLocate'] = locationInfo.LocationMap().reverseGeocodeLocal

    if args.columnshortlist:
        params['columnShortList'] = args.columnshortlist

    if args.tweetssincedate:
        if not args.timelines:
            print("You must use --time_lines with --tweets_since_date")
            sys.exit()
        params['tweetsSinceDate'] = datetime.strptime(args.tweetssincedate, "%Y-%m-%d")

    twtSQL = t.TwitterMySQL(**params)

    search_params = {'replace' : True,
                     'monthlyTables': args.monthlytables,
                     #'searchType': None,
                     'tweet_mode': 'extended',
                    }

    # grab tweets via bounding box
    if args.bb:
        if len(args.bb) < 3 or len(args.bb) > 4:
            print "There is something wrong with your bounding box. Must be LON LAT LON LAT or LON LAT RADIUS"
            sys.exit(1)
        if len(args.bb) == 3:
            search_params['geocode'] = ",".join(args.bb) + "mi"
            while True:
                twtSQL.searchToMySQL(**search_params)
        else:
            search_params['locations'] = ",".join(args.bb)
            while True:
                twtSQL.filterStreamToMySQL(**search_params)
    
    # get tweets from specific country:
    elif args.cbb and not (args.streamterms or args.streamfile):
        if len(args.cbb) > 25: 
            print("You can use at most 25 bounding boxes.")
            sys.exit(1)
        bboxes = list()
        for country_abbrv in args.cbb:
            try:
                country_name, box_coords = cbb.country_bounding_boxes[country_abbrv.upper()]
            except:
                print("Incorrect country code: %s." % country_abbrv)
                print("Try one of the following: %s." % ", ".join(sorted(cbb.country_bounding_boxes.keys())))
                sys.exit(1)
            
            bboxes.append(",".join([str(coord) for coord in box_coords]))

        search_params['locations'] = ",".join(bboxes)
        while True:
            twtSQL.filterStreamToMySQL(**search_params)

    # grab tweets via stream terms
    elif args.streamterms or args.streamfile:
        if args.streamfile:
            stream_terms = []
            with open(args.streamfile) as f:
                for term in f:
                    if term.isspace():
                        continue
                    stream_terms.append(term.replace('\n', ''))

            if not stream_terms:
                print "--search_term_file was set but no search terms were found."
                sys.exit(1)
            search_params['track'] = ",".join(stream_terms)
        else:
            if not args.streamterms:
                print "Please enter a search term."
                sys.exit(1)
            search_params['track'] = ",".join(args.streamterms)

        while True:
            twtSQL.filterStreamToMySQL(**search_params)

    # grab tweets via search terms
    elif args.searchterms:
        search_params['q'] = ",".join(args.searchterms)
        twtSQL.searchToMySQL(**search_params)

    # grab random tweets
    elif args.randomstream:
        while True:
            twtSQL.randomSampleToMySQL(**search_params)

    # grab timelines
    elif args.timelines:
        if args.trimuser:
            search_params['trim_user'] = 'true'
        if args.noretweets:
            search_params['include_rts'] = 'false'
        tt = 0
        for user in open(args.userlist):
            tt += 1
            user = user.rstrip()
            search_params = isScreeName(search_params, user)
            if "user_id" in search_params:
                user_params = {"user_id": search_params["user_id"]}
            else:
                user_params = {"screen_name": search_params["screen_name"]}

            print("########## User {tt}: {u}".format(tt=tt, u=user))
            try:
                user_object = twtSQL._apiRequestNoRetry(twitterMethod="users/show", params=user_params).next()
            except:
                user_object = False
                print("########## User {u} does not exist".format(u=user))
                continue
            if 'protected' in user_object and user_object['protected']:
                print("########## User {u} is private".format(u=user))
                continue
            else:
                twtSQL.userTimelineToMySQL(**search_params)
                


    # follow users
    elif args.followusers or args.followuserswithcron:
        if args.trimuser:
            search_params['trim_user'] = 'true'
        if args.noretweets:
            search_params['include_rts'] = 'false'

        if args.userlist:
            print("########## First pulling timelines for given users")
            for user in open(args.userlist):
                print("########## User {t}: {u}".format(t=total_users, u=user.rstrip('\n')))
                search_params = isScreeName(search_params, user.rstrip('\n'))
                twtSQL.userTimelineToMySQL(**search_params)
        if args.followusers:
            while True:
                user_msg_dict = dict(twtSQL.getUserMaxIDList())
                for user_id, last_message_id in user_msg_dict.items():
                    search_params['user_id'] = user_id
                    search_params['since_id'] = last_message_id
                    twtSQL.userTimelineToMySQL(**search_params)
        else:
            user_msg_dict = dict(twtSQL.getUserMaxIDList())
            for user_id, last_message_id in user_msg_dict.items():
                search_params['user_id'] = user_id
                search_params['since_id'] = last_message_id
                twtSQL.userTimelineToMySQL(**search_params)

    # pull tweets for a given message list
    elif args.messagelist:
        num_msgs = 0
        id_list = []
        for msgid in open(args.messagelist):
            num_msgs += 1
            id_list.append(str(msgid.rstrip('\r\n')))
            if num_msgs == 100:
                print("########## Pulling 100 messages")
                search_params['id'] = ','.join(id_list)
                twtSQL.messageIDsToMySQL(**search_params)
                num_msgs = 0
                id_list = []
        if num_msgs > 0:
            print("########## Pulling {n} messages".format(n=len(id_list)))
            search_params['id'] = ','.join(id_list)
            twtSQL.messageIDsToMySQL(**search_params)


    # grab profile pictures
    elif args.profilepictures:
        search_params['searchType'] = 'profile_pictures'
        search_params['outputName'] = createOutput(args.outputfile, "_pp")
        for user in open(args.userlist):
            search_params = isScreeName(search_params, user.rstrip('\n'))
            twtSQL.ppOrSNToOutput(**search_params)

    # grab social network
    elif args.socialnetwork:
        search_params['searchType'] = 'social_network'
        search_params['outputName'] = createOutput(args.outputfile, "_sn")
        for user in open(args.userlist):
            search_params = isScreeName(search_params, user.rstrip('\n'))
            twtSQL.ppOrSNToOutput(**search_params)

    else:
        print 'You are not grabbing anything.'

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("Time {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
# TODO
# get userlist from mysql database instead of file
# get tweets from lat lon radius
# dynamic wait function
