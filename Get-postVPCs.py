#! /usr/bin/python3
#  Read VPCs from AWS and display results

import boto3
import json
import jmespath
import os

vpcs = []
VpcIds = []
filters = []
jsonStr = '[\n' # begin the json object array


#############   Setup the Environment   ###############

HOME = os.environ.get('HOME')
if not os.path.exists('%s/%s'%(HOME,'data')):
    os.mkdir('%s/%s'%(HOME,'data'))
DATA_DIR = ('%s/%s'%(HOME,'data'))
###### Write AWS-VPC as a json file ###### 

# Capture the AWS-VPC as json objects
ec2 = boto3.resource( 'ec2', region_name='us-east-2')
client = boto3.client( 'ec2' )
filters = [{ 'Name':'tag:Name', 'Values':['default','ocm-train-vpc-devops-us']}]
[ec2.Vpc(id='vpc-504f318'), ec2.Vpc(id='vpc-02856bf1592004424')]
vpcs = list(ec2.vpcs.filter(Filters=filters))

# Post the AWS-VPCs as an array of json objects
with open('%s/%s'%(DATA_DIR,'Vpcs.json'),'w') as f:
    for vpc in vpcs:
        response = client.describe_vpcs( VpcIds = [vpc.id])

# Serialize the json objects; use a comma to delimit the objects 
        jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, separators=(',',': '))) 

    jsonStr = "}".join(jsonStr.rsplit("},", 1)) # Delete the last object delimiter (i.e., comma) in the object array

    f.write ('%s\n%s'%(jsonStr,']')) # Write the serialized json objects to a file and end the object array 

#    Load the json dictionary from the json file

with open('%s/%s'%(DATA_DIR,'Vpcs.json'),'r') as f:
#    if os.stat('%s/%s%'(DATA_DIR,'Vpcs')).st_size == 0:
#        print('%s %s'%('tmp','file is empty'))
    jsonVpcList = json.load(f)

###### Parse the json dictionary #####
#srchStr = "[*].Vpcs[*].CidrBlock"
srchStr = "[[*].Vpcs[0].Tags[*], [*].Vpcs[0].VpcId]"
with open('%s/%s'%(DATA_DIR,'VpcTags-Id.json'),'w') as f1:
    f1.write(json.dumps(jmespath.search(srchStr,jsonVpcList), sort_keys='True', indent=4, separators=(',',': ')))
exit()
