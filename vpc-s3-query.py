#! /usr/bin/python3
#  Read VPCs from AWS and display results

import boto3
import botocore
import json
import jmespath
import os
import re

vpcs = []
VpcIds = []
filters = []
jsonStr = '[\n' # begin the json object array


#############   Setup the Environment   ###############

os.environ['DEST_BUCK'] = 'ocm-train-s3.std-dev-us'
os.environ['DEST_DIR'] = 'Sys-mgt'
os.environ['DEST_KEY'] = 'vpc-obj'
os.environ['DEST_FILE'] = 'Vpcs.json'
#os.environ['TAG'] = 'Name'
#os.environ['FILTERS'] = 'default, ocm-train-vpc-devops-us'

###### Write AWS-VPC as a json file ###### 


# Capture the AWS-VPC as json objects
ec2 = boto3.resource( 'ec2', region_name='us-east-2')
client = boto3.client( 'ec2' )
filters = [{ 'Name':'tag:Name', 'Values':['default','ocm-train-vpc-devops-us']}]
[ec2.Vpc(id='vpc-504f318'), ec2.Vpc(id='vpc-02856bf1592004424')]
vpcs = list(ec2.vpcs.filter(Filters=filters))

# Post the AWS-VPCs as an array of json objects
for vpc in vpcs:
    response = client.describe_vpcs( VpcIds = [vpc.id])

# Serialize the json objects; use a comma to delimit the objects 
    jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, separators=(',',': '))) 
jsonStr = '%s%s'%("}".join(jsonStr.rsplit("},", 1)),']') # Delete the last comma and close the object array
jsonVpcList = json.loads(jsonStr)

s3 = boto3.resource('s3',region_name='us-east-2')
client = boto3.client( 's3' )
location = client.get_bucket_location(Bucket=os.environ['DEST_BUCK'])['LocationConstraint']
try:
    object = s3.Object(os.environ['DEST_BUCK'], '/%s/%s'%(os.environ['DEST_DIR'],os.environ['DEST_FILE']))
    object.load()
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        object.put( Body=(bytes(json.dumps(jsonVpcList).encode('UTF-8'))))
        url = "https://s3-%s.amazonaws.com/%s/%s"%(location, os.environ['DEST_BUCK'], os.environ['DEST_KEY'])
        print (url)
    else:
        # Something else has gone wrong.
        raise
else:
    response = input('/%s/%s %s'%(os.environ['DEST_DIR'], os.environ['DEST_FILE'], 'already exists, would you like to over-write it?\n'))
    if re.match('[Yy][Ee]?[Ss]?',response[0]):
        object.put( Body=(bytes(json.dumps(jsonVpcList).encode('UTF-8'))))
        url = "https://s3-%s.amazonaws.com/%s/%s"%(location, os.environ['DEST_BUCK'], os.environ['DEST_KEY'])
        print (url)
###### Parse the json dictionary #####
#srchStr = "[*].Vpcs[*].CidrBlock"
srchStr = "[[*].Vpcs[0].Tags[*], [*].Vpcs[0].VpcId]"
print(json.dumps(jmespath.search(srchStr,jsonVpcList), sort_keys='True', indent=4, separators=(',',': ')))
