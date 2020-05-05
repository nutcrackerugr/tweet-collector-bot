from abc import ABC, abstractmethod
import tweepy
import json
import datetime
import time
import logging

class OAuthKeys():
	
	_keys = list()
	_inuse = list()


	@classmethod
	def add(cls, ck, cs, k, s):
		OAuthKeys._keys.append((ck, cs, k, s))
		OAuthKeys._inuse.append(False)

	@classmethod
	def get(cls):
		for i in range(len(OAuthKeys._inuse)):
			if not OAuthKeys._inuse[i]:
				OAuthKeys._inuse[i] = True
				return OAuthKeys._keys[i]
		
		return None

	@classmethod
	def release(cls, key):
		for i in range(len(OAuthKeys._keys)):
			if key == OAuthKeys._keys[i]:
				OAuthKeys._inuse[i] = False
				return True
		
		return False
	
	@classmethod
	def from_file(cls, filename):
		with open(filename, 'r') as f:
			for line in f:
				key = tuple(line.strip().split(';'))
				
				OAuthKeys.add(*key)



class Collector(ABC):
	
	def __init__(self):
		self.credentials = OAuthKeys.get()
		
		if self.credentials:
			ck, cs, k, s = self.credentials
			self.auth = tweepy.OAuthHandler(ck, cs)
			self.auth.set_access_token(k, s)
		else:
			raise Exception("No free credentials")
		
		self.api = tweepy.API(
			self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
	
	def __del__(self):
		OAuthKeys.release(self.credentials)

	def query(self, q, limit=1000):
		return True
	
	def dump(self, q, limit=1000, folder="dumps", postfix="standard"):
		results = self.query(q, limit=limit)
		
		filepath = "{}/{}_{}.{}.json".format(
			folder, q, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
			postfix)
		with open(filepath, "w+") as f:
			json.dump(results, f, indent=2)


class StandardAPI(Collector):
	
	def query(self, q, limit=1000):
		results = list()

		if q[0] == "@":
			for page in tweepy.Cursor(
				self.api.user_timeline, id=q[1:],
				tweet_mode="extended").pages():
				for tweet in page:
					results.append(tweet._json)
				
		else:
			print("Querying with {}".format(q))
			for page in tweepy.Cursor(
				self.api.search, q=q, tweet_mode="extended").pages():
				for tweet in page:
					results.append(tweet._json)

					if limit != 0 and len(results) >= limit:
						return results
		
		return results


class StreamHandler(tweepy.StreamListener):
	def __init__(self, api=None, limit=0):
		self.results = list()
		self.limit = limit
		super().__init__(api=api)
		
	def on_status(self, status):
		self.results.append(status._json)

		if self.limit != 0 and len(self.results) >= self.limit:
			return False
		
	def on_error(self, status_code):
		if status_code == 420:
			return False


class StreamingAPI(Collector):
	def __init__(self):
		self.streamer = None
		self.stream = None
		self.last_q = None
		super().__init__()
	
	
	def query(self, q, limit=0):
		self.last_q = q
		
		self.streamer = StreamHandler(limit=limit)
		self.stream = tweepy.Stream(auth=self.api.auth, listener=self.streamer)
		
		self.stream.filter(track=[q], is_async=True)

	
	def disconnect(self):
		self.stream.disconnect()


	def dump(self, q=None, folder="dumps", postfix="stream"):
		q = q if q is not None else self.last_q
		results = self.streamer.results
		
		filepath = "{}/{}_{}.{}.json".format(
			folder, q, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
			postfix)
		with open(filepath, "w+") as f:
			json.dump(results, f, indent=2)



if __name__ == "__main__":
	OAuthKeys.from_file("credentials.txt")
	standard = StandardAPI()
	# streaming = StreamingAPI()
