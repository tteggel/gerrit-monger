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
crawl_state_db = db.crawl_state

crawl_state = crawl_state_db.find_one({'_id': 'change_list'})
if crawl_state is None:
    crawl_state = {'_id': 'change_list',
                   'last_sortKey': 'z'}
    crawl_state_db.insert(crawl_state)

def get_change_list(start='z', i=0):
    url = 'https://review.openstack.org/gerrit/rpc/ChangeListService'
    payload = {
        'jsonrpc': '2.0',
        'method': 'allQueryNext',
        'params': ['project:openstack/nova', start, 50],
        'id': i
    }
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
    }
    r = requests.post(url, data=json.dumps(payload), headers=headers).json()
    print(r)
    change_list = r['result']['changes']
    account_list = r['result']['accounts']

    for change in change_list:
        print(change['key']['id'])
        change['_id'] = change['id']
        try:
            change_db.insert(change)
        except DuplicateKeyError:
            pass #ignore already added changes
        crawl_state['last_sortKey'] = change['sortKey']
        crawl_state_db.update({'_id': 'change_list'}, crawl_state)

    if crawl_state['last_sortKey'] != None:
        sleep(randint(5, 10))
        get_change_list(start=crawl_state['last_sortKey'], i=i+1)

get_change_list(start=crawl_state['last_sortKey'])
