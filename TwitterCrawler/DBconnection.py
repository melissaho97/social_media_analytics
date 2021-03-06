import pymongo
import StreamingApiForUser
import utils
from pymongo import MongoClient
from bson.son import SON
import matplotlib.pylab as plt

from datetime import datetime, timedelta
from dateutil.parser import parse


class DBconnection:
    dburl = ""
    dbText = ""
    collectionString = ""

    def __init__(self, url, dbText, *args):
        self.dburl = url
        self.dbText = dbText
        if len(args) > 0:
            self.collectionString = args[0]

    def dbconnect_to_collection(self):

        try:
            myclient = pymongo.MongoClient(self.dburl)

            db = myclient[self.dbText]
            collection = db[self.collectionString]
            return collection
        except:
            print("fail to connect to collection")

    def insert_one_item(self, itemDictOne):

         x = self.dbconnect_to_collection().insert_one(itemDictOne)

        if x is not None:
            print("insert successfully")
        else:
            print("insert failed")

    def insert_many_item(self, itemDicts):

        x = self.dbconnect_to_collection().insert_one(itemDicts)


    @classmethod
    def count(cls,collection):

        count=0
        for elem in collection.find():
            count=count+1
        return count

    @classmethod
    def count_all(cls, collection):

        list = []
        resultslist = []
        count = 0
        time = []

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["created_at"])
        temp = starttime
        for elem in collection.find():
            nowtime = parse(elem["created_at"])
            if (nowtime - temp) < timedelta(minutes=10):
                count = count + 1
            elif (nowtime - temp) >= timedelta(minutes=10):
                list.append(count)
                count = 1
                temp = nowtime
                time.append(str(temp.strftime('%H:%M:%S')))
        return list, time

    @classmethod
    def count_all_google(cls, collection):

        list = []
        resultslist = []
        count = 0
        time = []

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["published"])
        temp = starttime
        for elem in collection.find():
            nowtime = parse(elem["published"])
            if -(nowtime - temp) < timedelta(minutes=10):
                count = count + 1
            elif -(nowtime - temp) >= timedelta(minutes=10):
                list.append(count)
                count = 1
                temp = nowtime
                time.append(str(temp.strftime('%H:%M:%S')))
        return list, time

    @classmethod
    def count_twitter_with_geotag(cls, collection):

        list = []
        resultslist = []
        count = 0
        time = []

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["created_at"])
        temp = starttime

        for elem in collection.find():
            nowtime = parse(elem["created_at"])
            if elem["geo"] != None:
                for e in elem["geo"]:
                    if (e == "Glasgow" or "glasgow") and (nowtime - temp) < timedelta(minutes=10):
                        count = count + 1
                        break
                    elif e == "Glasgow" or "glasgow" and (nowtime - temp) >= timedelta(minutes=10):
                        list.append(count)
                        count = 1
                        temp = nowtime
                        time.append(str(temp.strftime('%H:%M:%S')))
                        break
        return list, time

    @classmethod
    def count_twitter_with_geotag_google(cls, collection):

        list = []
        resultslist = []
        count = 0

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["published"])
        temp = starttime

        for elem in collection.find():
            if "location" in elem:
                for e in elem["location"].values():
                    if (e == "Glasgow" ):
                        count = count + 1
       return count

    @classmethod
    def count_retweet(cls, collection):

        list = []
        resultslist = []
        count = 0
        time = []

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["created_at"])
        temp = starttime

        for elem in collection.find():
            nowtime = parse(elem["created_at"])
            if elem["retweet_count"] == 0:
                continue
            elif elem["retweet_count"] != 0 and (nowtime - temp) < timedelta(minutes=10):
                count = count + 1
            elif elem["retweet_count"] != 0 and (nowtime - temp) >= timedelta(minutes=10):
                list.append(count)
                count = 1
                temp = nowtime
                time.append(str(temp.strftime('%H:%M:%S')))
        return list, time

    @classmethod
    def count_quotes(cls, collection):

        list = []
        resultslist = []
        count = 0
        time = []

        for elem in collection.find():
            resultslist.append(elem)
        starttime = parse(resultslist[0]["created_at"])
        temp = starttime

        for elem in collection.find():
            nowtime = parse(elem["created_at"])
            if "quoted_status" in elem:
                if elem["quoted_status"] == None:
                    continue
                elif elem["quoted_status"] != None and (nowtime - temp) < timedelta(minutes=5):
                    count = count + 1

                elif elem["quoted_status"] != None and (nowtime - temp) >= timedelta(minutes=5):
                    list.append(count)
                    count = 1
                    temp = nowtime
                    time.append(str(temp.strftime('%H:%M:%S')))

        return list, time

    @classmethod
    def redundant_tweets_count(cls, collec1, collec2):

        collection1 = collec1
        collection2 = collec2
        db = MongoClient().WEBSCIENCE
        pipeline = [
            {"$unwind": "$text"},
            {"$group": {"_id": "$text", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1), ("_id", -1)])}
        ]
        if collection1 == "twitter_REST_search_geo":
            a = list(db.twitter_REST_search_geo.aggregate(pipeline))
        if collection1 == "Twitter_location_with_tag":
            a = list(db.Twitter_location_with_tag.aggregate(pipeline))
        if collection1 == "Twitter_location_without_tag":
            a = list(db.Twitter_location_without_tag.aggregate(pipeline))
        if collection2 == "twitter_REST_search_geo":
            b = list(db.twitter_REST_search_geo.aggregate(pipeline))
        if collection2 == "Twitter_location_with_tag":
            b = list(db.Twitter_location_with_tag.aggregate(pipeline))
        if collection2 == "Twitter_location_without_tag":
            b = list(db.Twitter_location_without_tag.aggregate(pipeline))

        for Aelem in a:
            for belem in b:
                if Aelem["_id"] == belem["_id"]:
                    print(belem)


if __name__ == '__main__':
    a = DBconnection('mongodb://localhost:27017/', "WEBSCIENCE", "twitter_REST_search_geo")

    list, time = DBconnection.count_quotes(a.dbconnect_to_collection())
    plt.bar(range(len(list)), list)
    plt.xticks(range(len(time)), time)
    plt.savefig("countquote.pdf")
    plt.show()