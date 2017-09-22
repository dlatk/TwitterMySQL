#!/usr/bin/env python

import sys, os
import argparse
import json

from TwitterMySQL import TwitterMySQL as t
import TwitterMySQL.locationInfo as locationInfo


DEFAULT_OUTPUTFILE = 'twOutput'
DEFAULT_USERFILE = ''
DEFAULT_MONTHLY_TABLES = False
DEFAULT_DB = ''
DEFAULT_TABLE = 'msgs'
DEFAULT_AUTH = ''
DEFAULT_AUTH_JSON = ''
DEFAULT_BB = ''
DEFAULT_SEARCH_TERMS = ''
DEFAULT_RANDOM = False
DEFAULT_TL = False
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
	parser.add_argument('--time_lines', action='store_true', dest='timelines', default=DEFAULT_TL,
						help='Pull timelines for given list of user names / screen names.')
	parser.add_argument('--search_terms', type=str, metavar='FIELD(S)', dest='searchterms', nargs='*', default=DEFAULT_SEARCH_TERMS,
						help='Pull tweets containing keywords. Must supply space separated list of search terms. Ex: taylorswift #1989 #arianagrande')
	parser.add_argument('--search_term_file', dest='searchfile', default=None,
			                        help='Optional file containing list of search terms to use')

	parser.add_argument('--random_stream', action='store_true', dest='randomstream', default=DEFAULT_RANDOM,
					   help='Grab data from the random stream.')
	parser.add_argument('--social_network', action='store_true', dest='socialnetwork', default=DEFAULT_SN,
						help='Pull social network for given list of user names / screen names. Must specify path to file which contains list.')
	parser.add_argument('--profile_pictures', action='store_true', dest='profilepictures', default=DEFAULT_PP,
						help='Pull profile pictures for given list of user names / screen names. Must specify path to file which contains list.')
	parser.add_argument('--user_list', dest='userlist', default=DEFAULT_USERFILE,
						help='File containing list of user ids / screen names')
	parser.add_argument('--check_spam', dest='checkspam', default=False, action='store_true',
						help='Check each message for spam')

	args = parser.parse_args()

	# where are we writing everything? no database needed for profile pictures or social networks
	if not (args.profilepictures or args.socialnetwork ) and not args.db:
		print "You must specify a database"
		exit()

	# begin
	params = {'read_default_group': 'client',
			  'db': args.db,
			  'table': args.table,
			  }

	# can we authenticate? first try authfile, does it exist and is it properly formatted
	if args.authfile:
		if not os.path.isfile(args.authfile):
			print("Your authentication file %s does not exist. Use --auth path/to/file" % (args.authfile))
			exit()
		else:
			keys = [line.rstrip('\n') for line in open(args.authfile)]
			if len(keys) < 4:
				print "Something is wrong with your authentication file"
				exit()
			(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET) = keys
			params["API_KEY"] = CONSUMER_KEY
			params["API_SECRET"] = CONSUMER_SECRET
			params["ACCESS_TOKEN"] = ACCESS_TOKEN
			params["ACCESS_SECRET"] = ACCESS_SECRET
	# next try authJSON, does it exist and is it properly formatted
	elif args.authfileJSON:
		if not os.path.isfile(args.authfileJSON):
			print("Your authentication file %s does not exist. Use --authJSON path/to/file" % (args.authfile))
			exit()
		else:
			try:
				with open(args.authfileJSON) as data_file:
					cred_defs = json.load(data_file)
				params["authFile"] = args.authfileJSON
			except:
				print "Something is wrong with your authentication JSON file"
				exit()
	# no authentication possible, exit
	else:
		print("You must specify an authfile with either --auth or --authJSON")
		exit()

	if args.checkspam:
		params['checkSpam'] = True

	if args.bb:
		params['geoLocate'] = locationInfo.LocationMap().reverseGeocodeLocal

	twtSQL = t.TwitterMySQL(**params)

	search_params = {'replace' : True,
					 'monthlyTables': args.monthlytables,
					 #'searchType': None,
					}

	# grab tweets via bounding box
	if args.bb:
		if len(args.bb) < 3 or len(args.bb) > 4:
			print "There is something wrong with your bounding box. Must be LON LAT LON LAT or LON LAT RADIUS"
			exit()
		if len(args.bb) == 3:
			search_params['geocode'] = ",".join(args.bb) + "mi"
			while True:
				twtSQL.searchToMySQL(**search_params)
		else:
			search_params['locations'] = ",".join(args.bb)
			while True:
				twtSQL.filterStreamToMySQL(**search_params)

	# grab tweets via search terms
	elif args.searchterms or args.searchfile:
		if args.searchfile:
			search_terms = []
			with open(args.searchfile) as f:
				for term in f:
					if term.isspace():
						continue
					search_terms.append(term.replace('\n', ''))

			if not search_terms:
				print "--search_term_file was set but no search terms were found."
				exit()
			search_params['track'] = ",".join(search_terms)
		else:
			if not args.searchterms:
				print "Please enter a search term."
				exit()
			search_params['track'] = ",".join(args.searchterms)

		while True:
			twtSQL.filterStreamToMySQL(**search_params)

	# grab random tweets
	elif args.randomstream:
		while True:
			twtSQL.randomSampleToMySQL(**search_params)

	# grab timelines
	elif args.timelines:
		for user in open(args.userlist):
			search_params = isScreeName(search_params, user.rstrip('\n'))
			twtSQL.userTimelineToMySQL(**search_params)

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

# TODO
# get userlist from mysql database instead of file
# get tweets from lat lon radius
# dynamic wait function
