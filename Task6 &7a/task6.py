import tweepy
from tweepy import OAuthHandler
from kafka import KafkaProducer
import string

Topic='Bigdata-Task6'
#twitter api key
api_key = "xgVrEqna6O9Fxy0RFb2JEMrDK"
api_secret = "yef5MWMOBCEUGBQ8ijginr5iOJKRhCYGqIPmWz4FR1ZMXN70Ez"
access_token = "54177148-uUKrwhF2oDIjeRK3vlqGKNQGCsVVpRmShk1eANDGg"
access_secret = "JKpViMvXllVmxt803aD9CRak0wJQxEd6LF9R80Cx2RxGZ"

auth =OAuthHandler(api_key,api_secret)
auth.set_access_token(access_token,access_secret)
api = tweepy.API(auth)


Producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Words to track
keyw = ['Bitcoin', 'Currencies', 'Equities', 'Bonds']

class StdOutListener(tweepy.StreamListener):
    def on_connect(self):
        print("connection started to the twitter streaming")

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print("Error received in kafka producer " + repr(status_code))
            return True
            
        
    def on_status(self, status):
        Tuserfcnt = status.user.followers_count
        Tusersname = (status.user.screen_name).encode('utf-8')
        Twcreated = status.created_at
        Twtext = (status.text).encode('utf-8')
        Twtext = Twtext.translate(None, string.punctuation)
#        Twtext = re.sub(r"RT\s@\w*:\s", "", Twtext)
#        Twtext = re.sub(r"https?.*", "", Twtext)
        datastring = Tusersname + " === " + str(Tuserfcnt) + " === " +  \
                     str(Twcreated) + " === " + Twtext
        Producer.send(Topic, datastring)
        print('sending tweet ', datastring)
        return True    

listener = StdOutListener()
twitterstrm = tweepy.Stream(auth=api.auth, listener=listener)

twitterstrm.filter(track=keyw, languages=["en"])



