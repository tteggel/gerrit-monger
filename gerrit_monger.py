from random import randint
from time import sleep
import argparse

from pymongo import MongoClient
import gerritlib.gerrit

import version

###############################################################################
parser = argparse.ArgumentParser(
    description="""gerrit-monger - Crawls gerrit into mongodb (v{0}).""".format(version.get_version()))
parser.add_argument('-g', '--gerrithost', default='review.openstack.org',
                    help='hostname of the gerrit to crawl.',
                    type=str)
parser.add_argument('-p', '--gerritport', default=29418,
                    help='port of the gerrit to crawl.',
                    type=int)
parser.add_argument('-m', '--mongohost', default='127.0.0.1',
                    help='the hostname of the mongodb server.',
                    type=str)
parser.add_argument('-n', '--mongoport', default=27017,
                    help='the port number of the mongodb server.',
                    type=int)
parser.add_argument('-k', '--gerritkey', default='/home/ubuntu/.ssh/id_rsa',
                    help='location of the keyfile to log into the gerrit server.',
                    type=str)
parser.add_argument('-u', '--gerrituser', default='tteggel',
                    help='username for the gerrit server.',
                    type=str)
parser.add_argument('-r', '--resumesortkey', default='z',
                    help='start at the provided sort key.',
                    type=str)
args = parser.parse_args()

###############################################################################

changedb = MongoClient(args.mongohost, args.mongoport).openstack_gerrit.changes
gerrit = gerritlib.gerrit.Gerrit(args.gerrithost, args.gerrituser, 
                                 args.gerritport, args.gerritkey)

###############################################################################

def get_change_list(status):

    sortKey = args.resumesortkey
    last_sortKey = False	
    while sortKey != last_sortKey:
        last_sortKey = sortKey
        print(("Getting chages from gerrit - "
              "status: {0}; sortKey:{1}.").format(status, sortKey))
        try:
            changes = gerrit.bulk_query(("--commit-message --comments "
                                     "--current-patch-set --patch-sets "
                                     "--all-approvals --files --dependencies "
                                     "--submit-records limit:100 "
                                     "status:{0} resume_sortkey:{1}").format(
                                         status, sortKey))
        except Exception, e:
            print(e)

        for change in changes:
            if 'id' not in change: continue
            print('{0}: {1}'.format(
                change['project'], 
                change['id']))
            change['_id'] = change['id']
            changedb.save(change)
            if 'sortKey' in change:
                sortKey = change['sortKey']
        
	sleep(randint(5, 10))

###############################################################################

for status in [
    #'open', 
    #'reviewed', 
    #'submitted', 
    #'abandoned', 
    'merged'
    ]:
    get_change_list(status)
