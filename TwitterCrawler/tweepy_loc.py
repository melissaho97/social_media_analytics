from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from geopy.geocoders import Nominatim

import json
import gmplot
import twitter_credentials

# Authenticate using config.py and connect to Twitter Streaming API.
location = [-74.1687,40.5722,-73.8062,40.9467]
track = ['Trump', 'ocean', 'Democrat']
languages = ['en']
fetched_tweets_filename = "tweets_location_r.txt"

# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, location):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = Listener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth=auth, listener=ListenerChild(api=None,fetched_tweets_filename=fetched_tweets_filename),wait_on_rate_limit=True,wait_on_rate_limit_notify=True)


        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(locations=location)

 # # # # TWITTER STREAM LISTENER # # # #       
class Listener(StreamListener):
      1 == 1;

class ListenerChild(Listener):

      def __init__(self,api,fetched_tweets_filename):
          self.fetched_tweets_filename = fetched_tweets_filename
          super().__init__(api)

      def on_data(self, data):
        j = json.loads(data)
        try:
            if j['geo'] is not None:
                tt = parseTweets(j, simplify = FALSE, verbose = TRUE, legacy = FALSE)
                logging.info(tt)
                logging.info (tt)
                self.producer.send('tweets', bytearray(tt,'utf-8'))
        except KeyError:
            logging.info ("rate limited" + date.today().strftime('%Y-%m-%d %H:%M:%S'))

      def on_error(self, status):
        print(status)

if __name__ == '__main__':
    

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, location)
# auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
# auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)

# stream = Stream(auth=auth, listener=ListenerChild(api=None,fetched_tweets_filename=fetched_tweets_filename),wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

# stream.filter(locations=region,languages=languages,track=track)
