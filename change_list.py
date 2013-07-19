import json
from random import randint
from time import sleep

import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

m = MongoClient()
db = m.openstack_gerrit
change_db = db.changes
account_db = db.accounts

def get_change_list(start='z'):
    url = 'https://review.openstack.org/gerrit/rpc/ChangeListService'
    payload = {
        'jsonrpc': '2.0',
        'method': 'allQueryNext',
        'params': ['project:openstack/nova', start, 500],
        'id': 0
    }
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
    }
    r = requests.post(url, data=json.dumps(payload), headers=headers).json()
    change_list = r['result']['changes']

    last_change = 'z'

    for change in change_list:
        print(change['key']['id'])
        change['_id'] = change['id']
        try:
            change_db.insert(change)
        except DuplicateKeyError:
            pass #ignore already added changes
        last_change = change['sortKey']

    if last_change != 'z':
        sleep(randint(5, 10))
        get_change_list(start=last_change)

get_change_list()
