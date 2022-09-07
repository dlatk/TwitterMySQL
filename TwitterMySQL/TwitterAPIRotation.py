import time
import json
from TwitterAPI import TwitterAPI

class TwitterAPIRotation:
	"""Class to use multiple TwitterAPI connections to make requests

	Parameters
	----------

	param twapi_conn_list : list
			an array of TwitterAPI connection objects

	param twapi_cred_list_file :
		name of json file that contains a dict or list of user account info
	"""

	def __init__(self, twapi_conn_list = None, twapi_cred_list_file = None, twapi_token_list=None):
		# a list of lists
                # elemnts in the list are of the form - (twitterAPI object, time till reconnect, endpoint request counter)
		self.connections = []
		self.current_conn = -1

		assert not (twapi_conn_list and twapi_cred_list_file)
		if twapi_cred_list_file:
			twapi_conn_list = self._get_conn_list_from_json(twapi_cred_list_file)
		elif twapi_token_list:
			twapi_conn_list = [TwitterAPI(*twapi_token_list)]
		if twapi_conn_list:
			for conn in twapi_conn_list:
				self.register_connection(conn)

	def _get_conn_list_from_json(self, twapi_cred_list_file):
		with open(twapi_cred_list_file) as data_file:
			cred_defs = json.load(data_file)
			if type(cred_defs) is dict:
				cred_defs = cred_defs.values()
			conn_list = [TwitterAPI(**cred_def) for cred_def in cred_defs]
		return conn_list

	def _get_conn(self):
		return self.connections[self.current_conn]['connection']

	def _get_next_conn(self):
		self.current_conn = (self.current_conn + 1) % len(self.connections)
		return self._get_conn()

	def register_connection(self, connection):
		self.connections.append({'connection': connection, 'request_count': {}, 'wait_until': {}})

	def _wait_until(self, target_time):
		now = time.time()
		time_to_wait = target_time - now + 1
		while time_to_wait > 60:
			print "API limit encountered, waiting for %0.2f minutes..." % (float(time_to_wait)/60)
			time.sleep(60)
			now = time.time()
			time_to_wait = target_time - now + 1
		print "API limit encountered, waiting for %0.2f minutes..." % (float(time_to_wait)/60)
		time.sleep(time_to_wait)


	def request(self, twitterMethod, params):

		#try all the connections to see if I can get a non 429 response
		assert len(self.connections), "must have at least 1 connection"

		for i in range(0, len(self.connections)):
			conn = self._get_next_conn()

			if (twitterMethod not in self.connections[self.current_conn]['wait_until']) or \
			   (self.connections[self.current_conn]['wait_until'][twitterMethod] < time.time()):

				resp = conn.request(twitterMethod, params)
				if twitterMethod in self.connections[self.current_conn]['request_count']:
					self.connections[self.current_conn]['request_count'][twitterMethod] += 1
				else:
					self.connections[self.current_conn]['request_count'][twitterMethod] = 1

				if twitterMethod == 'statuses/user_timeline' and self.connections[self.current_conn]['request_count'][twitterMethod] == 100000:
					self.connections[self.current_conn]['request_count'][twitterMethod] = 0
					self.connections[self.current_conn]['wait_until'][twitterMethod] = time.time() + 24 * 3600
                                        print("100,000 request limit reached for statuses/user_timeline - waiting...")

				if resp.status_code == 429: #too many requests
					self.connections[self.current_conn]['wait_until'][twitterMethod] = float(resp.headers['x-rate-limit-reset'])
                                        print("Too many requests for", twitterMethod, "- waiting...")

				if resp.status_code != 429:
					return resp

                #if you didn't get any connection, wait for the one that's available the earliest
		self._wait_until(min([conn['wait_until'][twitterMethod] for conn in self.connections]))
		return self.request(twitterMethod, params)
