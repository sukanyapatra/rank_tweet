from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import tweepy
import csv
import datetime
from httplib import IncompleteRead

ckey = 'consumer key'
csecret = 'consumer secret'
atoken = 'access token'
asecret = 'access secret'

database_count = 0

def get_trends():
	auth1 = tweepy.OAuthHandler(ckey, csecret)
	auth1.set_access_token(atoken, asecret)
	api1 = tweepy.API(auth1)
	trends = api1.trends_place(1)
	data = trends[0] 
	# grab the trends
	trends = data['trends']
	# grab the name from each trend
	names=[]
	for trend in trends:
		names.append(trend['name'])
	
	# put all the names together with a ' ' separating them
	trendsName = ','.join(names)
	 
	return trendsName
	#threading.Timer(60, get_trends).start()
	
	


class listener(StreamListener):
	
	def __init__(self,api=None):
		super(listener, self).__init__()
		self.num_tweets = 0

	def on_data(self, data):
		try:
						
			all_data = json.loads(data)
			print(data)
			date1=all_data["created_at"].encode('utf-8')		
			date2=(all_data["retweeted_status"]["created_at"]).encode('utf-8')
			d1 = datetime.datetime.strptime(date1,'%a %b %d %H:%M:%S +0000 %Y')
			d2 = datetime.datetime.strptime(date2,'%a %b %d %H:%M:%S +0000 %Y')
			delta = d2-d1
			
			if((not(all_data["in_reply_to_user_id"])) and ('retweeted_status' in all_data) and (delta.total_seconds())<86400):
				retweeter_id = all_data["user"]["id_str"].encode('utf8')
				retweeter_screenname = (all_data["user"]["screen_name"]).encode('utf8')
				retweeter_followers = (all_data["user"]["followers_count"])
				original_tweet_text = (all_data["retweeted_status"]["text"]).encode('utf8')
				original_tweet_id = (all_data["retweeted_status"]["id_str"]).encode('utf8')
				original_user_id = (all_data["retweeted_status"]["user"]["id_str"]).encode('utf8')
				original_user_screen_name = (all_data["retweeted_status"]["user"]["screen_name"]).encode('utf8')
				original_user_followers = (all_data["retweeted_status"]["user"]["followers_count"])
				self.num_tweets += 1
				

				with open("database.csv","a") as file1:
					file1_writer = csv.writer(file1)
					file1_writer.writerow								([original_tweet_id,original_tweet_text,original_user_id,original_user_screen_name,original_user_followers,retweeter_id,retweeter_screenname,retweeter_followers])
				file1.close()
				
				#print "COUNT IS" + " " + str(self.num_tweets)
			if(self.num_tweets>5000):
				#print 'exceeded'
				return False
			else:
				return True
		except BaseException, e:
			print 'failed on_data',str(e)
			time.sleep(5)

	def on_error(self, status):
		print status
		return True
		
	def on_exception(self,status):
		return True
		
	def on_timeout(self,status):
		return True
		
	def on_disconnect(self,status):
		return True
		

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth)

list1=get_trends()
while(database_count<200):   #for storing 1 million tweets, 5000 at a time
	try:
		twitterStream = Stream(auth, listener())	
		twitterStream.filter(languages=["en"],track=[list1])
		database_count += 1
		list1 = get_trends()
		
	except:
		#print 'ERROR!'
		continue

