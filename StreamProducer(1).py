from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "UQIWSfCp4DEPgvqcjxqF4Zhtb"
consumer_secret = "eWaMpAVG2S0yMkOX6HwPH1w4txYLpghVIvLNDoHv7I5ohnVnrY"
access_token = "982813490229534720-79XWvMzGuEd36Mi3cZ6ob1i0376O8Vj"
access_secret = "b6j4pin83UNx6xwFdYwQl6wRzT67D9XLXrmaDARRMBukj"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



    def on_data(self, data):
        # Producer produces data for consumer

        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#trump'])
