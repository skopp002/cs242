# Reference: the following code is based off of the following tutorial https://www.youtube.com/watch?v=wlnx-7cm4Gg

#Importing necceray packages
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#used for creating an executable version.
import auto_py_to_exe

#credentials to access twitter dev account from separate file.
import twtrCreds

#We will be obtaining tweets via a stream...
# Class allowing streaming of live tweets
class TwitterStreamer():

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
# This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twtrCreds.CONSUMER_KEY, twtrCreds.CONSUMER_SECRET)
        auth.set_access_token(twtrCreds.ACCESS_TOKEN, twtrCreds.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

# This line filter Twitter Streams to capture data by the keywords:

        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename
# writes file with tweets, while the file is saved as txt, it is a json file.
    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # Proper variables are indicated...Authentication occurs using credentials in a separate file, the file is not
    # included for privacy/security reasons, however the exe version of this program includes the credentials.
    hash_tag_list = ["a", "the", "i"]
    fetched_tweets_filename = "tweets.txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
