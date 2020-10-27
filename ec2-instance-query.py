#! /usr/bin/python3

import boto3
import json
import jmespath
import os

#! /usr/bin/python3
#  Request and filter INSTANCES from AWS EC2 Service and post results to "Instances.json"
#  INSTANCES are filtered by Tags[Name/Value Pairs]
#  Then uses JmesPath to Query the "Instances.json" file. Results are posted to "InstancesQuery.json".
#  Options interface is planned as a future enhancement.

#  Recommenmded USAGE:  Obtain a working version of the python code from github.
#  Make a personal copy. Filter the instances coming from EC2 service using the examples shown in comments.  
#  Query the "Instances.json" file using the examples shown in comments.  


instances = []
InstanceIds = []
filters = []
jsonStr = '[\n' # begin the json object array for serializing json objects.

#############   Setup the Environment   ###############

HOME = os.environ.get('HOME')
if not os.path.exists('%s/%s'%(HOME,'data')):
    os.mkdir('%s/%s'%(HOME,'data'))
DATA_DIR = ('%s/%s'%(HOME,'data'))

ec2 = boto3.resource( 'ec2', region_name='us-east-2')
client = boto3.client( 'ec2' )

############# Post EC2-INSTANCEs to a "Instances.json" file ############# 

# Request and filter the INSTANCEs as json objects

filters = [ { 'Name':'tag:Name', 'Values': [ 'ocm*'] } ] 
#filters = [ { 'Name':'tag:Name', 'Values': [ 'ocm-train-ubuntu2004.ebs.t2.micro1-dev-us', 'ocm-train-ec2(ub2004.ebs.t2.mic2)vpc(cust1)-dev-us' ] } ] 

[ ec2.Instance(id = 'i-054b01cb726c32afd'), ec2.Instance(id = 'i-037345e13d2095e99') ]

instances = list(ec2.instances.filter(Filters=filters))
#instances = list(ec2.instances.all())
print (instances)
# exit() 

# Post the EC2-INSTANCEs as an array of json objects
with open('%s/%s'%(DATA_DIR,'Instances.json'),'w') as f:
    for instance in instances:
        response = client.describe_instances( InstanceIds = [instance.id])

# Serialize the json objects; use a comma to delimit the objects 
        jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, default=str, separators=(',',': '))) 

    jsonStr = "}".join(jsonStr.rsplit("},", 1)) # Delete the last object delimiter (i.e., comma) in the object array

    f.write ('%s\n%s'%(jsonStr,']')) # Write the serialized json objects to a file and end the object array 

#    Load the json dictionary from the json file

with open('%s/%s'%(DATA_DIR,'Instances.json'),'r') as f:
#    if os.stat('%s/%s%'(DATA_DIR,'Instances')).st_size == 0:
#        print('%s %s'%('tmp','file is empty'))
    jsonInstanceList = json.load(f)

###### Parse the json dictionary #####
#srchStr = "[*].Instances[*].CidrBlock"
#srchStr = "[0].Reservations[0].Instances[0].Tags[0].Key"
srchList` = ["[*].Reservations[*].Instances[0].Tags[0].Key", "[*].Reservations[*].Instances[0].Tags[0].Value"]
with open('%s/%s'%(DATA_DIR,'InstanceQuery.json'),'w') as f1:
    f1.write(json.dumps(jmespath.search(srchList[*],jsonInstanceList), sort_keys='True', indent=4, default=str, separators=(',',': ')))
exit()



