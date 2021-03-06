#! /usr/bin/python3
#  Read VPCs from AWS and display results

import boto3
import botocore
import json
import jmespath
import os
import re


#############   Intialize varaibles ###############
vpcs = []
VpcIds = []

#############   Setup the Environment   ###############

REGION = 'us-east-2'

EC2_SERVICE = 'ec2'
TAG = 'Name'
VPCS = ['default', 'ocm-train-vpc-devops-us']
FILTERS = [{ 'Name':'%s%s'%('tag:',TAG), 'Values':VPCS}]

S3_SERVICE = 's3'
OBJECT_BUCK = 'ocm-train-s3.std-dev-us'
OBJECT_ACL = 'public-read'
OBJECT_DIR = 'Sys-mgt'
OBJECT_FORMAT = 'json'
OBJECT_NAME = '%s.%s'%('Vpcs',OBJECT_FORMAT)
OBJECT_KEY = '%s/%s/%s'%(OBJECT_ACL,OBJECT_DIR,OBJECT_NAME)
#GrantRead = 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

# connect with the ec2.Vpc service
ec2 = boto3.resource( EC2_SERVICE, region_name = REGION)
client = boto3.client( EC2_SERVICE )

#############   Get the VPCs; serialize as a json array ###############

[ec2.Vpc(id='vpc-504f318'), ec2.Vpc(id='vpc-02856bf1592004424')]
vpcs = list(ec2.vpcs.filter(Filters=FILTERS))
jsonStr = '[\n' # begin the json object array
for vpc in vpcs:
    response = client.describe_vpcs( VpcIds = [vpc.id])
    jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, default=str, separators=(',',': '))) 
jsonStr = '%s%s'%("}".join(jsonStr.rsplit("},", 1)),']') # Delete the last comma and close the object array

# connect with the S3 service
s3 = boto3.resource( S3_SERVICE, region_name = REGION )
client = boto3.client( S3_SERVICE )

#############   Post the VPCs to S3 -- a json array publically readable on the Internet ###############

location = client.get_bucket_location(Bucket=OBJECT_BUCK)['LocationConstraint']
try:
    object = s3.Object(OBJECT_BUCK, OBJECT_KEY)
    object.load()
except botocore.exceptions.ClientError as e: 
    if e.response['Error']['Code'] == "404": # file not found
        client.put_object( 
            Body = json.dumps(jsonStr, sort_keys='True', indent=4, default=str, separators=(',',': ')), 
            Bucket = OBJECT_BUCK,
            Key = OBJECT_KEY)
        client.put_object_acl( Bucket = OBJECT_BUCK, Key = OBJECT_KEY, ACL = OBJECT_ACL)
        url = "https://s3-%s.amazonaws.com/%s/%s"%(location, OBJECT_BUCK, OBJECT_KEY)
        print (url)
    else:
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

#############   Interactively parse the VPCs using jmespath and the IPython shell ###############
#srchStr = "[*].Vpcs[*].CidrBlock"
jsonVpcList = json.loads(jsonStr)
srchStr = "[[*].Vpcs[0].Tags[*], [*].Vpcs[0].VpcId]"
print(json.dumps(jmespath.search(srchStr,jsonVpcList), sort_keys='True', indent=4, default=str, separators=(',',': ')))
