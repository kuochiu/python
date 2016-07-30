###############################
# Programmer: Nugget          #
# Date: 07/28/2016            #
###############################

#!/usr/bin/env python
import boto3
import botocore

# amazon s3 client for bucket
class s3client:
    """Amazon S3 client to deliver log file to the bucket"""
    # initialize
    def __init__(self,keyid,accesskey,bucket,folder):
        self._keyid = keyid
        self._accesskey = accesskey
        self._bucket = bucket
        self._folder = folder

    #deliver method to put the log file into amazon s3 bucket
    def deliver(self):
        client =  boto3.client('s3', aws_access_key_id=self._keyid, aws_secret_access_key=self._accesskey)
        objects = client.list_objects(Bucket = self._bucket)['Contents']
        for object in objects:
            print object['Key']
        filename = 'test5.txt'
        try:
            client.put_object(Bucket = self._bucket,Body = filename, Key= self._folder+"/"+filename)
            print "Uploaded to Amazon S3 successfully!"
        except botocore.exceptions.ClientError as e:
            print int(e.response['Error']['Code'])
            #    print "Failed to upload due to invalid access key ID"

        print "Error bypassed"

# http client to do post
print "http client"
