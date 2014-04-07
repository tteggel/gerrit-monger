from random import randint
from time import sleep
import argparse
import pprint

from pymongo import MongoClient
import gerritlib.gerrit
import yaml
import requests

import version

###############################################################################
parser = argparse.ArgumentParser(
    description="""gerrit-monger - Crawls gerrit into mongodb (v{0}).""".format(version.get_version()))
parser.add_argument('-g', '--gerrithost', default='review.openstack.org',
                    help='hostname of the gerrit to crawl.',
                    type=str)
parser.add_argument('-i', '--gerritport', default=29418,
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
parser.add_argument('-p', '--projects', default=None,
                    help='projects to mong.',
                    type=str, nargs='*')
parser.add_argument('-a', '--age', default=None,
                    help=('only return changes that have changed in last '
                          'N s, m, h, d, w, mon, y.'),
                    type=str)
args = parser.parse_args()

###############################################################################

changedb = MongoClient(args.mongohost, args.mongoport).openstack_gerrit.changes
gerrit = gerritlib.gerrit.Gerrit(args.gerrithost, args.gerrituser, 
                                 args.gerritport, args.gerritkey)

###############################################################################

def get_change_list(project, age=None, sort_key=None):

    last_sort_key = False	
    while sort_key != last_sort_key:
        last_sort_key = sort_key
        print(("Getting chages from gerrit - "
              "project: {0}; sortKey: {1}.").format(project, sort_key))
        try:
            changes = gerrit.bulk_query(("--commit-message --comments "
                                     "--current-patch-set --patch-sets "
                                     "--all-approvals --files --dependencies "
                                     "--submit-records limit:100 -- "
                                     "project:{0} resume_sortkey:{1} "
                                     "{2}").format(project, sort_key, 
                                       "-age:{0}".format(age) 
                                         if age else ""))
        except Exception, e:
            print(e)

        for change in changes:
            if 'id' not in change: continue
            print('{3} is {2} in {0} at {1}'.format(
                change['project'], change['sortKey'], change['status'], 
                change['number']))
            change['_id'] = change['id']
            changedb.save(change)
            if 'sortKey' in change:
                sort_key = change['sortKey']
        
	sleep(randint(5, 10))


def get_approved_projects():
    programs_url = 'http://git.openstack.org/cgit/openstack/governance/plain/reference/programs.yaml'
    y = yaml.load(requests.get(programs_url).text)

    projects = []
    for _, project in y.items():
        for _, l in project['projects'].items():
            for repo in l:
                projects.append(repo)

    return projects
    

###############################################################################

projects = []
if args.projects:
    projects = args.projects
else:
    projects = get_approved_projects() 

pprint.pprint(projects)

for project in projects:
    get_change_list(project, age=args.age, 
        sort_key=args.resumesortkey)
