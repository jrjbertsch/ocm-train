#! /usr/bin/python3

import boto3
import botocore
import jmespath
import json
import os
import re

#  Request and filter interesting 'ec2' Instances from AWS's 'ec2' Service.
#  Instances are filtered using 'ec2' Tags[Name/Value Pairs].
#  Interesting Instances are sent to S3 as a list of json objects
#  Interesting Instances are queried -- JmesPath 
#  Options interface is planned as a future enhancement.

#############   Intialize varaibles ###############

instances = []
InstanceIds = []

#############   Setup the Environment   ###############

REGION = 'us-east-2'

EC2_SERVICE = 'ec2'
TAG = 'Name'
INSTANCES = ['ocm-train-ubuntu2004.ebs.t2.micro1-dev-us', 'ocm-train-ec2(ub2004.ebs.t2.mic2)vpc(cust1)-dev-us' ] 
FILTERS = [{ 'Name':'%s%s'%('tag:',TAG), 'Values':INSTANCES}]
#INSTANCE_PATTERN = [ 'ocm*' ]
#FILTERS = [{ 'Name':'%s%s'%('tag:',TAG), 'Values':INSTANCE_PATTERN}]

S3_SERVICE = 's3'
OBJECT_BUCK = 'ocm-train-s3.std-dev-us'
OBJECT_ACL = 'public-read'
OBJECT_DIR = 'Sys-mgt'
OBJECT_FORMAT = 'json'
OBJECT_NAME = '%s.%s'%('Instances',OBJECT_FORMAT)
OBJECT_KEY = '%s/%s/%s'%(OBJECT_ACL,OBJECT_DIR,OBJECT_NAME)

# connect with the EC2 service; use instance 'Tags' to filter interesting instances
ec2 = boto3.resource( EC2_SERVICE, region_name = REGION)
client = boto3.client( EC2_SERVICE )
[ ec2.Instance(id = 'i-054b01cb726c32afd'), ec2.Instance(id = 'i-037345e13d2095e99') ]
instances = list(ec2.instances.filter(Filters=FILTERS))

#############   Serialize the interesting Instances ###############

jsonStr = '[\n' # open the json object array for serializing json objects.
for instance in instances:
    response = client.describe_instances( InstanceIds = [instance.id])
    jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, default=str, separators=(',',': ')))
jsonStr = '%s%s'%("}".join(jsonStr.rsplit("},", 1)),']') # Delete the last comma and close the object array

# connect with the S3 service
s3 = boto3.resource( S3_SERVICE, region_name = REGION )
client = boto3.client( S3_SERVICE )

#############   Post the Instances to S3 -- a json array publically readable on the Internet ###############

location = client.get_bucket_location(Bucket=OBJECT_BUCK)['LocationConstraint']
try:
    s3.Object(OBJECT_BUCK, OBJECT_KEY).load()
except botocore.exceptions.ClientError as e: 
    if e.response['Error']['Code'] == "404": # file not found
        client.put_object( 
            Body = json.dumps(jsonStr, sort_keys='True', indent=4, default=str, separators=(',',': ')), 
            Bucket = OBJECT_BUCK,
            Key = OBJECT_KEY)
        client.put_object_acl( Bucket = OBJECT_BUCK, Key = OBJECT_KEY, ACL = OBJECT_ACL)
        url = "https://s3-%s.amazonaws.com/%s/%s"%(location, OBJECT_BUCK, OBJECT_KEY)
        print (url)
    else: # Exception
        raise
else: # file found
    response = input('/%s/%s %s'%(OBJECT_BUCK, OBJECT_KEY, 'already exists, would you like to over-write it?\n'))
    if re.match('[Yy][Ee]?[Ss]?',response):
        client.put_object( 
            Body = json.dumps(jsonStr, sort_keys='True', indent=4, default=str, separators=(',',': ')), 
            Bucket = OBJECT_BUCK,
            Key = OBJECT_KEY)
        client.put_object_acl( Bucket = OBJECT_BUCK, Key = OBJECT_KEY, ACL = OBJECT_ACL)
        url = 'https://s3-%s.amazonaws.com/%s/%s'%(location, OBJECT_BUCK, OBJECT_KEY)
        print (url)

#############   Query the json dictionary ###############

jsonInstanceList = json.loads(jsonStr)
srchStr = "[*].Reservations[*].Instances[0].Tags[0]"
print(json.dumps(jmespath.search(srchStr,jsonInstanceList), sort_keys='True', indent=4, default=str, separators=(',',': ')))
