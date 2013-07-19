import json
from random import randint
from time import sleep

import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

m = MongoClient()
db = m.openstack_gerrit
change_db = db.changes

def get_change_detail(id, i=0):
    url = 'https://review.openstack.org/gerrit/rpc/ChangeDetailService'
    payload = {
        'jsonrpc': '2.0',
        'method': 'changeDetail',
        'params': [{'id':id}],
        'id': i
    }
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
    }
    r = requests.post(url, data=json.dumps(payload), headers=headers).json()
    return r['result']

changes = change_db.find({'$or': [
    {'change': {'$exists': False}},
    {'change.open': True}}]})
for change in changes:
    if 'change' not in change:
        detail = get_change_detail(change['_id']['id'])
        if detail != None:
            detail['_id'] = change['_id']
            change_db.update({'_id': change['_id']}, detail)
    sleep(randint(5, 10))
