#! /usr/bin/python

__author__ = "Maarten Sap, Salvatore Giorgi"
__email__ = "maartensap93@gmail.com, sal.giorgi@gmail.com"
__version__ = "0.4"


"""
TODOs:
pull retweeted message as well as original
better geolocation?

"""

import datetime, time
import os, sys
import json, re
import traceback
import urllib

import MySQLdb
from TwitterAPIRotation import TwitterAPIRotation
from requests.exceptions import ChunkedEncodingError

import xml.etree.ElementTree as ET
from HTMLParser import HTMLParser


MAX_MYSQL_ATTEMPTS = 5
MAX_TWITTER_ATTEMPTS = 5
TWEET_LIMIT_BEFORE_INSERT = 200
TWT_REST_WAIT = 15*60

SPAM_LIST = ["share", "win", "check", "enter", "products", "awesome", "prize", "sweeps", "bonus", "gift"]

DEFAULT_MYSQL_COL_DESC = ["message_id bigint(20) primary key",
							"user_id bigint(20)",
							"message text",
							"created_at_utc datetime",
							"retweeted varchar(8)",
							"retweet_message_id bigint(20)",
							"in_reply_to_message_id bigint(20)",
							"in_reply_to_user_id bigint(20)",
							"favorite_count int(6)",
							"retweet_count int(6)",
							"source varchar(128)",
							"message_lang varchar(8)",
							"user_handle varchar(128)",
							"user_desc text",
							"user_lang varchar(16)",
							"time_zone varchar(64)",
							"utc_offset int(11)",
							"friends_count int(6)",
							"followers_count int(6)",
							"user_location varchar(256)",
							"street_address varchar(256)",
							"region varchar(128)",
							"postal_code int(5)",
							"bb_coordinates varchar(256)",
							"country varchar(128)",
							"country_code varchar(8)",
							"tweet_location varchar(128)",
							"tweet_location_short varchar(128)",
							"place_type varchar(128)",
							"coordinates varchar(128)",
							"coordinates_state varchar(3)",
							"index useriddex (user_id)",
							"index datedex (created_at_utc)"]

DEFAULT_TWEET_JSON_SQL_CORR = {'message_id': "['id_str']",
								'user_id': "['user']['id_str']",
								'message': "['text']",
								'created_at_utc': "['created_at']",
								'retweeted': "['retweeted']",
								'retweet_message_id': "['retweeted_status']['id']",
								'in_reply_to_message_id': "['in_reply_to_status_id_str']",
								'in_reply_to_user_id': "['in_reply_to_user_id_str']",
								'favorite_count': "['favorite_count']",
								'retweet_count': "['retweet_count']",
								'source': "['source']",
								'message_lang': "['lang']",
								'user_handle': "['user']['screen_name']",
								'user_desc': "['user']['description']",
								'user_lang': "['user']['lang']",
								'time_zone': "['user']['id_str']",
								'utc_offset': "['user']['utc_offset']",
								'friends_count': "['user']['friends_count']",
								'followers_count': "['user']['followers_count']",
								'user_location': "['user']['location']",
								'street_address': "['place']['attributes']['street_address']",
								'region': "['place']['attributes']['region']",
								'postal_code': "['place']['attributes']['postal_code']",
								'bb_coordinates': "['place']['bounding_box']['coordinates']",
								'country': "['place']['country']",
								'country_code': "['place']['country_code']",
								'tweet_location': "['place']['full_name']",
								'tweet_location_short': "['place']['name']",
								'place_type': "['place']['place_type']" }

TWEET_DATE_LOCATION = 3

class TwitterMySQL:
	"""Wrapper for the integration of Twitter APIs into MySQL
	Turns JSON tweets into row format
	Failsafe connection to MySQL servers
	Geolocates if tweet contains coordinates in the US
	[TODO] Geolocates using the Google Maps API

	Parameters
	----------
	- db              MySQL database to connect to
	- table           table to insert Twitter responses in
	- API_KEY         Twitter API key (to connect to Twitter)
	- API_SECRET      Twitter API Secret
	- ACCESS_TOKEN    Twitter App Access token
	- ACCESS_SECRET   Twitter App Access token secret
	
	Optional parameters:
	- authFile        list of files containing Twitter API keys
	- noWarnings      disable MySQL warnings [Default: False]
	- checkSpam   check each message for spam (does message contain
						words from SPAM_LIST)
						[Default: False]
	- dropIfExists    set to True to delete the existing table
	- geoLocate       a function that converts coordinates to state
						and/or address.
						Format:
						(state, address) = your_method(lat, long)
	- errorFile       error logging file - warnings will be written to it
						[Default: stderr]
	- jTweetToRow     JSON tweet to MySQL row tweet correspondence
						(see help file for more info)
						[Default: DEFAULT_TWEET_JSON_SQL_CORR]
	- SQLfieldsExp    SQL column description for MySQL table
						[Default: DEFAULT_MYSQL_COL_DESC]
	- host            host where the MySQL database is on
						[Default: localhost]
	- columnShortList	List of MySQL columns to save, must correspond
						to keys in DEFAULT_TWEET_JSON_SQL_CORR
	- any other MySQL.connect argument
	"""

	def _warn(self, *objs):
		errorStream = open(self.errorFile, "a+") if self.errorFile else sys.stderr
		print >> errorStream, "\rWARNING: ", " ".join(str(o) for o in objs)

	def __init__(self, **kwargs):

		if "table" in kwargs:
			self.table = kwargs["table"]
			del kwargs["table"]
		else:
			raise ValueError("Table name missing")

		if "dropIfExists" in kwargs:
			self.dropIfExists = kwargs["dropIfExists"]
			del kwargs["dropIfExists"]
		else:
			self.dropIfExists = False

		if "geoLocate" in kwargs:
			self.geoLocate = kwargs["geoLocate"]
			del kwargs["geoLocate"]
		else:
			self.geoLocate = None

		if "noWarnings" in kwargs and kwargs["noWarnings"]:
			del kwargs["noWarnings"]
			from warnings import filterwarnings
			filterwarnings('ignore', category = MySQLdb.Warning)

		if "errorFile" in kwargs:
			self.errorFile = kwargs["errorFile"]
			del kwargs["errorFile"]
		else:
			self.errorFile = None

		if "jTweetToRow" in kwargs:
			self.jTweetToRow = kwargs["jTweetToRow"]
			del kwargs["jTweetToRow"]
		else:
			if "columnShortList" in kwargs and kwargs["columnShortList"]:
				self.jTweetToRow = {k:v for k,v in DEFAULT_TWEET_JSON_SQL_CORR.items() if k in kwargs["columnShortList"]}
			else:
				self.jTweetToRow = DEFAULT_TWEET_JSON_SQL_CORR

		if "fields" in kwargs and "SQLfieldsExp" in kwargs:
			# Fields from the JSON Tweet to pull out
			self.columns = kwargs["fields"]
			del kwargs["fields"]
			self.columns_description = kwargs["SQLfieldsExp"]
			del kwargs["SQLfieldsExp"]
			if len([f for f in self.columns_description if "index" != f[:5]]) != len(self.columns):
				raise ValueError("There was a mismatch between the number of columns in the 'fields' and the 'field_expanded' variable. Please check those and try again.")

		elif "fields" in kwargs:
			raise ValueError("Please provide a detailed MySQL column description of the fields you want grabbed. (keyword argument: 'SQLfieldsExp')")

		elif "SQLfieldsExp" in kwargs:
			self.columns_description = kwargs["SQLfieldsExp"]
			del kwargs["SQLfieldsExp"]
			self.columns = [f.split(' ')[0]
							for f in self.columns_description
							if f.split(' ')[0][:5] != "index"]
		else:
			if "columnShortList" in kwargs and kwargs["columnShortList"]:
				self.columns_description = [i for i in DEFAULT_MYSQL_COL_DESC if i.split()[0] in kwargs["columnShortList"]]
				del kwargs["columnShortList"]
			else:
				self.columns_description = DEFAULT_MYSQL_COL_DESC
			self.columns = [f.split(' ')[0]
							for f in self.columns_description
							if f.split(' ')[0][:5] != "index"]

		if "api" in kwargs:
			self._api = kwargs["api"]
			del kwargs["api"]
		elif ("API_KEY" in kwargs and
				"API_SECRET" in kwargs and
				"ACCESS_TOKEN" in kwargs and
				"ACCESS_SECRET" in kwargs and
				"authFile" not in kwargs):
			self._api = TwitterAPIRotation(twapi_token_list=[kwargs["API_KEY"], kwargs["API_SECRET"], kwargs["ACCESS_TOKEN"], kwargs["ACCESS_SECRET"]])
			del kwargs["API_KEY"], kwargs["API_SECRET"], kwargs["ACCESS_TOKEN"], kwargs["ACCESS_SECRET"]
		elif ("authFile" in kwargs):
			self._api = TwitterAPIRotation(twapi_cred_list_file=kwargs["authFile"])
			del kwargs["authFile"]
		else:
			raise ValueError("TwitterAPI object or API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET needed to connect to Twitter. Please see dev.twitter.com for the keys.")

		if not "charset" in kwargs:
			kwargs["charset"] = 'utf8mb4'

		if "checkSpam" in kwargs:
			self.columns_description.append("spam int(1)")
			self.columns.append("spam")
			del kwargs["checkSpam"]

		if "tweetsSinceDate" in kwargs:
			self.tweets_since_date = kwargs["tweetsSinceDate"].date()
			del kwargs["tweetsSinceDate"]
		else:
			self.tweets_since_date = ""

		try:
			self._connect(kwargs)
		except TypeError as e:
			print "You're probably using the wrong keywords, here's a list:\n"+self.__init__.__doc__
			raise TypeError(e)

	def _connect(self, kwargs = None):
		"""Connecting to MySQL sometimes has to be redone"""
		if kwargs:
			self._SQLconnectKwargs = kwargs
		elif not kwargs and self._SQLconnectKwargs:
			kwargs = self._SQLconnectKwargs

		self._connection = MySQLdb.connect(**kwargs)
		self.cur = self._connection.cursor()

	def _wait(self, t, verbose = True):
		"""Wait function, offers a nice countdown"""
		for i in xrange(t):
			if verbose:
				print "\rDone waiting in: %s" % datetime.timedelta(seconds=(t-i)),
				sys.stdout.flush()
			time.sleep(1)
		if verbose:
			print "\rDone waiting!           "

	def _execute(self, query, nbAttempts = 0, verbose = True):
		if nbAttempts >= MAX_MYSQL_ATTEMPTS:
			self._warn("Too many attempts to execute the query, moving on from this [%s]" % query[:300])
			return 0

		if verbose: print "SQL:\t%s" % query[:200]

		try:
			ret = self.cur.execute(query)
		except Exception as e:
			if "MySQL server has gone away" in str(e):
				self._connect()
			nbAttempts += 1
			if not verbose: print "SQL:\t%s" % query[:200]
			self._warn("%s [Attempt: %d]" % (str(e), nbAttempts))
			self._wait(nbAttempts * 2)
			ret = self._execute(query, nbAttempts, False)

		return ret

	def _executemany(self, query, values, nbAttempts = 0, verbose = True):
		if nbAttempts >= MAX_MYSQL_ATTEMPTS:
			self._warn("Too many attempts to execute the query, moving on from this [%s]" % query[:300])
			return 0

		if verbose: print "SQL:\t%s" % query[:200]
		ret = None
		try:
			#print query, values[0]
			ret = 0
			for i in xrange(len(values)):
				ret += self.cur.execute(query, values[i])
		except Exception as e:
			if "MySQL server has gone away" in str(e):
				self._connect()
				nbAttempts += 1
				if not verbose: print "SQL:\t%s" % query[:200]
				self._warn("%s [Attempt: %d]" % (str(e), nbAttempts))
				self._wait(nbAttempts * 2)
				ret = self._executemany(query, values, nbAttempts, False)
			else:
				traceback.print_exc()

		return ret

	def createTable(self, table = None):
		"""
		Creates the table specified during __init__().
		By default, the table will be deleted if it already exists,
		but there will be a 10 second grace period for the user
		to cancel the deletion (by hitting CTRL-c).
		To disable the grace period and have it be deleted immediately,
		please use dropIfExists = True during construction
		"""
		table = self.table if not table else table

		# Checking if table exists
		SQL = """show tables like '%s'""" % table
		self._execute(SQL)
		SQL = """create table %s (%s) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin""" % (table, ', '.join(self.columns_description))
		if not self.cur.fetchall():
			# Table doesn't exist
			self._execute(SQL)
		else:
			# table does exist
			if not self.dropIfExists:
				USER_DELAY = 10

				for i in xrange(USER_DELAY):
					print "\rTable %s already exists, it will be deleted in %s, please hit CTRL-C to cancel the deletion" % (table, datetime.timedelta(seconds=USER_DELAY-i)),
					sys.stdout.flush()
					time.sleep(1)

			print "\rTable %s already exists, it will be deleted" % table, " " * 150
			SQL_DROP = """drop table %s""" % table
			self._execute(SQL_DROP)
			self._execute(SQL)

	def insertRow(self, row, table = None, columns = None, verbose = True):
		"""Inserts a row into the table specified using an INSERT SQL statement"""
		return self.insertRows([row], table, columns, verbose)

	def replaceRow(self, row, table = None, columns = None, verbose = True):
		"""Inserts a row into the table specified using a REPLACE SQL statement."""
		return self.replaceRows([row], table, columns, verbose)

	def insertRows(self, rows, table = None, columns = None, verbose = True):
		"""Inserts multiple rows into the table specified using an INSERT SQL statement"""
		table = self.table if not table else table
		columns = self.columns if not columns else columns

		EXISTS = "SHOW TABLES LIKE '%s'" % table
		if not self._execute(EXISTS, verbose = False): self.createTable(table)

		SQL = u"INSERT INTO %s (%s) VALUES (%s)" % (table,
													 ', '.join(columns),
													 ', '.join("%s" for r in rows[0]))
		#print columns

		rows = map(lambda x:tuple(x), rows)

		#print SQL
		#print rows[0]
		#print SQL.format(rows[0])

		return self._executemany(SQL, rows, verbose = verbose)

	def replaceRows(self, rows, table = None, columns = None, verbose = True):
		"""Inserts multiple rows into the table specified using a REPLACE SQL statement"""
		table = self.table if not table else table
		columns = self.columns if not columns else columns

		EXISTS = "SHOW TABLES LIKE '%s'" % table
		if not self._execute(EXISTS, verbose = False): self.createTable(table)

		SQL = "REPLACE INTO %s (%s) VALUES (%s)" % (table,
													', '.join(columns),
													', '.join("%s" for r in rows[0]))
		return self._executemany(SQL, rows, verbose = verbose)

	def executeGetUserList(self, table = None):
		"""Executes a given query, expecting one resulting column. Returns results as a list"""
		table = self.table if not table else table
		SQL = "SELECT DISTINCT user_id FROM %s" % (table)
		return map(lambda x:x[0], self._execute(SQL).fetchall())

	def getUserMaxIDList(self, table = None):
		"""Executes a given query, expecting one resulting column. Returns results as a list"""
		table = self.table if not table else table
		SQL = """SELECT t1.user_id, t1.message_id FROM {table} t1 WHERE t1.created_at_utc = (SELECT MAX(t2.created_at_utc) FROM {table} t2 WHERE t2.user_id = t1.user_id)""".format(table=self.table)
		self._execute(SQL)
		return map(lambda x:(int(x[0]),int(x[1])), self.cur.fetchall())


	def _tweetTimeToMysql(self, timestr):
		# Mon Jan 25 05:02:27 +0000 2010
		return str(time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(timestr, '%a %b %d %H:%M:%S +0000 %Y')))

	def _yearMonth(self, mysqlTime):
		return time.strftime("%Y_%m",time.strptime(mysqlTime,"%Y-%m-%d %H:%M:%S"))

	def _prepTweet(self, jTweet):
		tweet = {}
		for SQLcol in self.columns:
			try:

				if SQLcol in self.jTweetToRow:
					if SQLcol == "message":
						if 'extended_tweet' in jTweet:
							self.jTweetToRow[SQLcol] = "['extended_tweet']['full_text']"
						elif 'full_text' in jTweet:
							self.jTweetToRow[SQLcol] = "['full_text']"
						elif 'text' in jTweet:
							self.jTweetToRow[SQLcol] = "['text']"
						else:
							print("Neither 'text' nor 'full_text' in Tweet json. You possibly have empty messages")
							print(jTweet)
							exit()
					tags = ''
					for tag in re.split(r'\[(.*?)\]', self.jTweetToRow[SQLcol]):
						if tag == '':
							continue
						#print tag
						tag = '[' + tag + ']'
						tags += tag
						if eval("jTweet%s" % tags) is None or not eval("jTweet%s" % tags):
							tweet[SQLcol] = None
							break

					else:
						#successful: values can be found
						try:
							result = eval("jTweet%s" % self.jTweetToRow[SQLcol])
							if SQLcol == "message" and 'retweeted_status' in jTweet:
								# get full text of original tweet and add to retweet
								try:
									if 'extended_tweet' in jTweet['retweeted_status']:
										orig_tweet_text =  jTweet['retweeted_status']['extended_tweet']['full_text']
									elif 'full_text' in jTweet['retweeted_status']:
										orig_tweet_text =  jTweet['retweeted_status']['full_text']
									elif 'text' in jTweet['retweeted_status']:
										orig_tweet_text =  jTweet['retweeted_status']['text']

									result = result.split(orig_tweet_text.split()[0],1)[0]+orig_tweet_text
								except:
									pass
							if isinstance(result, str):
								result = unicode(result)
								result = self._connection.escape_string(result)

							tweet[SQLcol] = result

							#tweet[SQLcol] = self._connection.escape_string(eval("jTweet%s" % self.jTweetToRow[SQLcol]))

						except KeyError, TypeError:
							tweet[SQLcol] = None
							continue
						except Exception as e:
							print "error: ", str(e)
							print traceback.print_exc()
							raise


					#place attributes cannot exist if ['place'] is null
		#                    if self.jTweetToRow[SQLcol].startswith('[\'place\']') and jTweet['place'] is None:
		#                        tweet[SQLcol] = None
		#                        continue
		#
		#                    if self.jTweetToRow[SQLcol].startswith('[\'place\'][\'attributes\']') and (jTweet['place']['attributes'] is None or not jTweet['place']['attributes']):
		#                        tweet[SQLcol] = None
		#                        print self.jTweetToRow[SQLcol]
		#                        print jTweet['place']
		#                        continue

					#tweet[SQLcol] = eval("jTweet%s" % self.jTweetToRow[SQLcol])

					if isinstance(tweet[SQLcol], str) or isinstance(tweet[SQLcol], unicode):
						tweet[SQLcol] = HTMLParser().unescape(tweet[SQLcol]).encode("utf-8")
					if SQLcol == "created_at_utc":
						if self.tweets_since_date and datetime.datetime.strptime(self._tweetTimeToMysql(tweet[SQLcol]), "%Y-%m-%d %H:%M:%S").date() < self.tweets_since_date:
							return 'all_tweets_since_date'
						tweet[SQLcol] = self._tweetTimeToMysql(tweet[SQLcol])
					if SQLcol == "source":
						try:
							tweet[SQLcol] = ET.fromstring(re.sub("&", "&amp;", tweet[SQLcol])).text
						except Exception as e:
							raise NotImplementedError("OOPS", type(e), e, [tweet[SQLcol]])
					if SQLcol == "bb_coordinates":
						tweet[SQLcol] = str(tweet[SQLcol])

					# check for spam
					if SQLcol == "message" and "spam" in self.columns:
						if any(substring in tweet["message"] for substring in SPAM_LIST):
							tweet["spam"] = 1
						else:
							tweet["spam"] = 0

				else:
					if SQLcol != "spam":
						tweet[SQLcol] = None
			except KeyError:
				tweet[SQLcol] = None


		if not any(tweet.values()):
			raise NotImplementedError("OOPS", jTweet, tweet)

		# Coordinates state and address
		if "coordinates" in jTweet and jTweet["coordinates"]:
			lon, lat = map(lambda x: float(x), jTweet["coordinates"]["coordinates"])
			if self.geoLocate:
				(state, address) = self.geoLocate(lat, lon)
			else:
				(state, address) = (None, None)
			tweet["coordinates"] = str(jTweet["coordinates"]["coordinates"])
			tweet["coordinates_state"] = str(state) if state else None
			tweet["coordinates_address"] = str(address) if address else str({"lon": lon, "lat": lat})

		# Tweet is dictionary of depth one, now has to be linearized
		tweet = [tweet[SQLcol] for SQLcol in self.columns]
		return tweet

	def _apiRequest(self, twitterMethod, params):
		done = False
		nbAttempts = 0

		while not done and nbAttempts < MAX_TWITTER_ATTEMPTS:
			try:
				r = self._api.request(twitterMethod, params)

			except Exception as e:
			# If the request doesn't work
				if "timed out" in str(e).lower():
					self._warn("Time out encountered, reconnecting immediately.")
					self._wait(1, False)
				else:
					self._warn("Unknown error encountered: [%s]" % str(e))
					self._wait(10)
				nbAttempts += 1
				continue

			# Request was successful in terms of http connection
			try:
				for i, response in enumerate(r.get_iterator()):
					# Checking for error messages
					if isinstance(response, int) or "delete" in response:
						continue
					if "limit" in response:
						missed = response["limit"]["track"]
						print("\nMissed " + str(missed) + " so far due to rate limiting.")
						continue
					if i == 0 and "message" in response and "code" in response:
						if response['code'] == 88: # Rate limit exceeded
							self._warn("Rate limit exceeded, waiting 15 minutes before a restart")
							self._wait(TWT_REST_WAIT)
						else:
							self._warn("Error message received from Twitter %s" % str(response))
						continue
					if "searchType" in params and params["searchType"]:
						yield response
					else:
						yield self._prepTweet(response)
				done = True
			except ChunkedEncodingError as e:
				# nbAttempts += 1
				self._warn("ChunkedEncodingError encountered, reconnecting immediately: [%s]" % e)
				continue
			except Exception as e:
				nbAttempts += 1
				self._warn("unknown exception encountered, waiting %d second: [%s]" % (nbAttempts * 2, str(e)))
				traceback.print_exc()
				self._wait(nbAttempts * 2)
				continue
			# If it makes it all the way here, there was no error encountered
			nbAttempts = 0

		if nbAttempts >= MAX_TWITTER_ATTEMPTS:
			self._warn("Request attempted too many times (%d), it will not be executed anymore [%s]" % (nbAttempts, twitterMethod + str(params)))
			return

	def _apiRequestNoRetry(self, twitterMethod, params):
		try:
			r = self._api.request(twitterMethod, params)

		except Exception as e:
		# If the request doesn't work
			if "timed out" in str(e).lower():
				self._warn("Time out encountered, reconnecting immediately.")
			else:
				self._warn("Unknown error encountered: [%s]" % str(e))

		# Request was successful in terms of http connection
		try:
			for i, response in enumerate(r.get_iterator()):
				yield response
		except ChunkedEncodingError as e:
			self._warn("ChunkedEncodingError encountered, reconnecting immediately: [%s]" % e)
		except Exception as e:
			self._warn("unknown exception encountered: [%s]" % (str(e)))
			traceback.print_exc()

	def apiRequestNoRetry(self, twitterMethod, **params):
		"""
		Takes in a Twitter API request and yields formatted responses in return

		Use as follows:
		for tweet in twtSQL.apiRequest('statuses/filter', track="Twitter API"):
			print tweet

		For more info (knowing which twitterMethod to use) see:
		http://dev.twitter.com/rest/public
		http://dev.twitter.com/streaming/overview
		"""
		return self._apiRequestNoRetry(twitterMethod, params)


	def apiRequest(self, twitterMethod, **params):
		"""
		Takes in a Twitter API request and yields formatted responses in return

		Use as follows:
		for tweet in twtSQL.apiRequest('statuses/filter', track="Twitter API"):
			print tweet

		For more info (knowing which twitterMethod to use) see:
		http://dev.twitter.com/rest/public
		http://dev.twitter.com/streaming/overview
		"""
		for response in self._apiRequest(twitterMethod, params):
			yield response

	def _tweetsToMySQL(self, tweetsYielder, replace = False, monthlyTables = False):
		"""
		Tool function to insert tweets into MySQL tables in chunks,
		while outputting counts.
		"""
		tweetsDict = {}
		i = 0

		# TWEET_LIMIT_BEFORE_INSERT = 100

		for tweet in tweetsYielder:
			i += 1

			try:
				tweetsDict[self._yearMonth(tweet[TWEET_DATE_LOCATION])].append(tweet)
			except KeyError:
				tweetsDict[self._yearMonth(tweet[TWEET_DATE_LOCATION])] = [tweet]

			if i % 10 == 0:
				print "\rNumber of tweets grabbed: %d" % i,
				sys.stdout.flush()

			if i % TWEET_LIMIT_BEFORE_INSERT == 0:
				print
				if monthlyTables:
					for yearMonth, twts in tweetsDict.iteritems():
						table = self.table+"_"+yearMonth
						if replace:
							print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, table, self.replaceRows(twts, table = table, verbose = False), time.strftime("%c"))
						else:
							print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(twts, table = table, verbose = False), table, time.strftime("%c"))
				else:
					tweets = [twt for twts in tweetsDict.values() for twt in twts]
					if replace:
						print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, self.table, self.replaceRows(tweets, verbose = False), time.strftime("%c"))
					else:
						print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
				i, tweetsDict = (0, {})

		# If there are remaining tweets
		if any(tweetsDict.values()):
			print
			if monthlyTables:
				for yearMonth, twts in tweetsDict.iteritems():
					table = self.table+"_"+yearMonth
					if replace:
						print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, table, self.replaceRows(twts, table = table, verbose = False), time.strftime("%c"))
					else:
						print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(twts, table = table, verbose = False), table, time.strftime("%c"))
			else:
				tweets = [twt for twts in tweetsDict.values() for twt in twts]
				if replace:
					print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, self.table, self.replaceRows(tweets, verbose = False), time.strftime("%c"))
				else:
					print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
			i, tweetsDict = (0, {})

	def tweetsToMySQL(self, twitterMethod, **params):
		"""
		Ultra uber awesome function that takes in a Twitter API
		request and inserts it into MySQL, all in one call

		Here's some examples on how to use it:
			For the Search API
			twtSQL.tweetsToMySQL('search/tweets', q='"Taylor Swift" OR "Jennifer Lawrence"')

			For hydrating (getting all available details) for a tweet
			twtSQL.tweetsToMySQL('statuses/lookup', id="504710715954188288")

		For more twitterMethods and info on how to use them, see:
		http://dev.twitter.com/rest/public
		http://dev.twitter.com/streaming/overview
		"""

		# Replace SQL command instead of insert
		if "replace" in params:
			replace = params["replace"]
			del params["replace"]
		else:
			replace = False
		if "monthlyTables" in params:
			monthlyTables = params["monthlyTables"]
			del params["monthlyTables"]
		else:
			monthlyTables = False

		self._tweetsToMySQL(self._apiRequest(twitterMethod, params), replace = replace, monthlyTables = monthlyTables)
		return

	def randomSampleToMySQL(self, **params):
		"""
		Takes the random sample of all tweets (~ 1%) and
		inserts it into monthly table [tableName_20YY_MM].
		For more info, see:
		http://dev.twitter.com/streaming/reference/get/statuses/sample
		"""
		self.tweetsToMySQL('statuses/sample', **params)

	def filterStreamToMySQL(self, **params):
		"""
		Use this to insert the tweets from the FilterStream into MySQL

		Here's an example:
			twtSQL.filterStreamToMySQL(track="Taylor Swift")
		Here's a second example (Continental US bounding box):
			twtSQL.filterStreamToMySQL(locations="-124.848974,24.396308,-66.885444,49.384358")

		More info here:
		http://dev.twitter.com/streaming/reference/post/statuses/filter
		"""
		twitterMethod =  'search/tweets' if 'geocode' in params else 'statuses/filter'
		self.tweetsToMySQL(twitterMethod, **params)

	def userTimeline(self, **params):
		"""
		For a given user, returns all the accessible tweets from that user,
		starting with the most recent ones (Twitter imposes a 3200 tweet limit).

		Here's an example of how to use it:
		for tweet in userTimeline(screen_name = "taylorswift13"):
			print tweet

		See http://dev.twitter.com/rest/reference/get/statuses/user_timeline for details
		"""
		ok = True
		print "Finding tweets for %s" % ', '.join(str(k)+': '+str(v) for k,v in params.iteritems())
		params["count"] = 200 # Twitter limits to 200 returns

		i = 0

		while ok:

			tweets = list()
			for tweet in self._apiRequest('statuses/user_timeline', params):
				if tweet == "all_tweets_since_date":
					ok = False
					continue
				tweets.append(tweet)
			if not tweets:
				# Warn about no tweets?
				if "max_id" in params:
					self._warn("No more tweets! Max_id: %s" % (params["max_id"]))
				ok = False
				if i != 0: print
			else:
				i += len(tweets)

				print "\rNumber of tweets grabbed: %d" % i,
				sys.stdout.flush()
				print tweets[-1][0]
				params["max_id"] = str(long(tweets[-1][0])-1)
				for tweet in tweets:
					yield tweet

	def userTimelineToMySQL(self, **params):
		"""
		For a given user, inserts all the accessible tweets from that user into,
		the current table. (Twitter imposes a 3200 tweet limit).

		Here's an example of how to use it:
		userTimelineToMySQL(screen_name = "taylorswift13")

		For details on keywords to use, see
		http://dev.twitter.com/rest/reference/get/statuses/user_timeline
		"""
		print "Grabbing users tweets and inserting into MySQL"

		# Replace SQL command instead of insert
		if "replace" in params:
			replace = params["replace"]
			del params["replace"]
		else:
			replace = False

		if "monthlyTables" in params:
			monthlyTables = params["monthlyTables"]
			del params["monthlyTables"]
		else:
			monthlyTables = False

		self._tweetsToMySQL(self.userTimeline(**params), replace = replace, monthlyTables = monthlyTables)

	def messageIDs(self, **params):
		"""
		For a given user, returns all the accessible tweets from that user,
		starting with the most recent ones (Twitter imposes a 3200 tweet limit).

		Here's an example of how to use it:
		for tweet in userTimeline(screen_name = "taylorswift13"):
			print tweet

		See http://dev.twitter.com/rest/reference/get/statuses/user_timeline for details
		"""
		tweets = [tweet for tweet in self._apiRequest('statuses/lookup', params)]
		for tweet in tweets:
			yield tweet	

	def messageIDsToMySQL(self, **params):
		"""
		For a given list of ids, inserts all the accessible tweets 
		the current table. (Twitter imposes a 3200 tweet limit).

		For details on keywords to use, see
		https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/get-statuses-lookup
		"""
		print "Grabbing tweets from a list of ids and inserting into MySQL"

		# Replace SQL command instead of insert
		if "replace" in params:
			replace = params["replace"]
			del params["replace"]
		else:
			replace = False

		if "monthlyTables" in params:
			monthlyTables = params["monthlyTables"]
			del params["monthlyTables"]
		else:
			monthlyTables = False

		self._tweetsToMySQL(self.messageIDs(**params), replace = replace, monthlyTables = monthlyTables)

	def search(self, **params):
		"""
		Search API
		"""
		ok = True
		print "Finding tweets for %s" % ', '.join(str(k)+': '+str(v) for k,v in params.iteritems())
		params["count"] = 200 # Twitter limits to 200 returns

		i = 0

		while ok:

			tweets = [tweet for tweet in self.apiRequest('search/tweets', **params)]
			if not tweets:
				# Warn about no tweets?
				ok = False
				if i != 0: print
			else:
				i += len(tweets)

				print "\rNumber of tweets grabbed: %d" % i,
				sys.stdout.flush()

				params["max_id"] = str(long(tweets[-1][0])-1)
				for tweet in tweets:
					yield tweet


	def searchToMySQL(self, **params):
		"""
		Queries the Search API and pulls as many results as possible

		Here's an example of how to use it:
		userTimelineToMySQL(screen_name = "taylorswift13")

		For details on keywords to use, see
		http://dev.twitter.com/rest/reference/get/statuses/user_timeline
		"""
		print "Grabbing users tweets and inserting into MySQL"

		# Replace SQL command instead of insert
		if "replace" in params:
			replace = params["replace"]
			del params["replace"]
		else:
			replace = False

		if "monthlyTables" in params:
			monthlyTables = params["monthlyTables"]
			del params["monthlyTables"]
		else:
			monthlyTables = False

		self._tweetsToMySQL(self.search(**params), replace = replace, monthlyTables = monthlyTables)

	def _nonTweetsToOutput(self, tweetsYielder, replace = False, monthlyTables = False, method = '', outputName = ''):
		"""
		Tool function to write non-tweet data (social networks and profile images) to output ,
		while outputting counts.
		"""
		tweetsDict = {}
		i = 0

		# TWEET_LIMIT_BEFORE_INSERT = 100

		for tweet in tweetsYielder:
			i += 1

			if method == 'profile_pictures':
				user_id = tweet['id']
				image_url = tweet['profile_image_url'].replace("_normal", "_bigger")
				write_file = "/".join([outputName, str(user_id) + ".jpg"])
				urllib.urlretrieve(image_url, write_file)
			else:
				write_file = "/".join([outputName, tweet['user'] + ".json"])
				with open(write_file, 'w') as f: json.dump(tweet, f)

		#               if i % 10 == 0:
		#                       print "\rNumber of tweets grabbed: %d" % i,
		#                       sys.stdout.flush()

		#               if i % TWEET_LIMIT_BEFORE_INSERT == 0:
		#                       print
		#                       tweets = [twt for twts in tweetsDict.values() for twt in twts]
		#                       #print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
		#                       print "Sucessfully inserted %4d tweets into '%s' [%s]" % (tweets, outputName, time.strftime("%c"))
		#                       i, tweetsDict = (0, {})

		# # If there are remaining tweets
		# if any(tweetsDict.values()):
		#       print
		#       tweets = [twt for twts in tweetsDict.values() for twt in twts]
		#       #print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
		#       print "Sucessfully inserted %4d tweets into '%s' [%s]" % (tweets, outputName, time.strftime("%c"))
		#       i, tweetsDict = (0, {})

	def ppOrSNToOutput(self, **params):
		"""
		"""

		if "replace" in params:
			replace = False
			del params["replace"]
		else:
			replace = False

		if "monthlyTables" in params:
			monthlyTables = False
			del params["monthlyTables"]
		else:
			monthlyTables = False

		if "outputName" in params:
			outputName = params["outputName"]
			del params["outputName"]
		else:
			outputName = False

		if "searchType" in params:
			searchType = params["searchType"]
		else:
			searchType = False

		if searchType == "profile_pictures":
			append = "_pp"
		elif searchType == "":
			append = "_sn"
		else:
			append = ""

		self._nonTweetsToOutput(self.userNetworkOrPicture(**params), replace = replace, monthlyTables = monthlyTables, method = searchType, outputName = outputName)

	def userNetworkOrPicture(self, **params):
		"""
		"""
		ok = True
		print "Finding tweets for %s" % ', '.join(str(k)+': '+str(v) for k,v in params.iteritems())
		#params["count"] = 200 # Twitter limits to 200 returns

		i = 0
		if params["searchType"] == 'profile_pictures':
			yield self._apiRequest('users/show', params).next()

		elif params["searchType"]  == 'social_network':
			user = params['screen_name'] if 'screen_name' in params else params['user_id']

			#get followers
			followers_list = list()
			cursor = -1
			while cursor != 0:
				i += 1
				response = self._apiRequest('followers/ids', params).next()
				followers_list.extend(response['ids'])
				cursor = response['next_cursor']
				print "\rNumber of followers grabbed: %d" % (len(followers_list)),
				sys.stdout.flush()
			print

			# get friends
			friends_list = list()
			cursor = -1
			while cursor != 0:
				i += 1
				response = self._apiRequest('friends/ids', params).next()
				friends_list.extend(response['ids'])
				cursor = response['next_cursor']
				print "\rNumber of friends grabbed: %d" % (len(friends_list)),
				sys.stdout.flush()
			print

			yield {'user': user, 'followers_list': followers_list, 'friends_list': friends_list}
		else:
			print "You're probably using the wrong search type"
			exit()
