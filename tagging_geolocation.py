from pymongo import MongoClient
from pygeocoder import Geocoder
from elasticsearch import Elasticsearch
import time

mongo_client = MongoClient()
db = mongo_client.pune_bus
collection = db.routes
collection_insert = db.routes_with_geolocation_v4

for i in collection.find(no_cursor_timeout= True):
    if 'route_up' in i:
        for j in i['route_up']:
            temp_dict = {}
            try:
                geocode = Geocoder.geocode(j['stop_name'] + ",Pune")
                latitude,longitude = geocode.coordinates
                temp_dict['latitude'] = latitude
                temp_dict['longitude'] = longitude
            except Exception, e:
                temp_dict['latitude'] = None
                temp_dict['longitude'] = None
            j.update(temp_dict)
            time.sleep(0.05)
    if 'route_down' in i:
        for k in i['route_down']:
            temp1_dict = {}
            try:
                geocode = Geocoder.geocode(k['stop_name'] + ",Pune")
                latitude,longitude = geocode.coordinates
                temp_dict1['latitude'] = latitude
                temp_dict1['longitude'] = longitude
            except Exception, e:
                temp_dict1['latitude'] = None
                temp_dict1['longitude'] = None
            k.update(temp_dict1)
            time.sleep(0.05)
    i['route_name'] = i['route_name'].strip()
    i['route_number'] = i['route_number'].strip()
    print i['route_number']
    collection_insert.insert(i)
