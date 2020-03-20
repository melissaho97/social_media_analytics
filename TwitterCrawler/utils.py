import tweepy
import time
import DBconnection
import pymongo
import twitter_credentials


#authenticate the app
auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)

# get the api
api = tweepy.API(auth)



def search():
    return api.search('Glasgow', lang="en")


def dataCrawler():
    a = DBconnection.DBconnection('mongodb://localhost:27017/', "WEBSCIENCE", "twitter_REST_search_geo")
    while 1:
        while 1:
            try:
                # thread1
                resultset = search()
                for result in resultset:
                    itemDicts = result._json
                    try:
                        a.insert_many_item(itemDicts)
                        print(result._json)
                    except pymongo.errors.DuplicateKeyError:
                        continue
            except tweepy.RateLimitError:
                print("api hit the limit")
                time.sleep(60 * 1)
                break

        while 1:
            try:
                # theard2
                resultset1 = search1()
                for result1 in resultset1:
                    itemDicts1 = result1._json
                    try:
                        a.insert_many_item(itemDicts1)
                        print(result1._json)
                    except pymongo.errors.DuplicateKeyError:
                        continue
            except tweepy.RateLimitError:
                print("api1 hit the limit")
                time.sleep(60 * 1)
                break

        time.sleep(60 * 8)


if __name__ == '__main__':
    dataCrawler()