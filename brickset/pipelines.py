# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo

#from scrapy.conf import settings
from scrapy.exceptions import DropItem
#from scrapy import log
import logging

class BricksetPipeline(object):
    def process_item(self, item, spider):
        return item


class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient("localhost",27017)
        db = connection["brickset"]
        self.collection = db["sets"]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert(dict(item))
#            log.msg("Question added to MongoDB database!", level=log.DEBUG, spider=spider)
            logging.log(logging.DEBUG, "Question added to MongoDB database!")
        return item