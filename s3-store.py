#! /usr/bin/python3

import json
import boto3    
import os    

#############   Setup the Environment   ###############

HOME = os.environ.get('HOME')
if not os.path.exists('%s/%s'%(HOME,'data')):
    os.mkdir('%s/%s'%(HOME,'data'))
SRC_DIR = ('%s/%s'%(HOME,'data'))
DEST_DIR = ('/%s'%('Sys-Mgt'))

s3 = boto3.resource('s3')

if not os.path.exists('%s/%s'%(SRC_DIR,'Vpcs.json')):
    print (':%s/%s:%s'%(SRC_DIR,'Vpcs.json','file not found'))
    exit()

############## Put Json file into an s3 bucket  #####

with open('%s/%s'%(SRC_DIR,'Vpcs.json'),'r') as f:
    vpcs = json.load(f)
    
s3O = s3.Object('s3://ocm-train-s3.std-dev-us', '%s/%s'%(DEST_DIR,'Vpcs.json'))

s3O.put(
    Body=(bytes(json.dumps(vpcs).encode('UTF-8')))
    )
