import inspect

import blist
from boto.exception import *
from smart_open import *

import config

aws_access_key_id = config.aws['acceskey']
aws_secret_access_key = config.aws['secretkey']


def cleanS3(filename=None):
    self.processing_finished = False
    try:
        bucket = boto.connect_s3(self.aws_access_key_id, self.aws_secret_access_key).get_bucket(
            self.cfg_folder_bucket)
        for key, content in s3_iter_bucket(bucket, accept_key=lambda key: key.startswith(filename),
                                           workers=int(self.cfg_streaming_thread)):
            msg = "\r -> Deleting S3 Files: %s " % str(key)
            sys.stdout.write(msg)
            sys.stdout.flush()
            key.delete()
        self.processing_finished = True
        print("")
    except (BotoServerError, BotoClientError, S3ResponseError, Exception) as e:
        print("Error on delete %s: " % e)


def cleanS3ALL():
    bucket = boto.connect_s3(self.aws_access_key_id, self.aws_secret_access_key).get_bucket(
        self.cfg_folder_bucket)
    bucketListResultSet = bucket.list()
    for key in bucketListResultSet:
        bucket.delete_keys(key.name)
        print("%s deleted..." % key.name)


def saveS3(row, filename):
    # uri = `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`
    uri = "s3://%s:%s@%s" % (aws_access_key_id, aws_secret_access_key, filename)
    print("Salving data on bucket %s" % filename)
    try:
        amount_line = 0
        with smart_open(uri, 'wb') as fout:
            # OR RESULT SET...
            if type(row) is list or type(row) is tuple or type(row) is blist or inspect.isgenerator(row):
                for line in row:
                    if not line == None:
                        line = str(line) + '\n'
                        size = len(line)
                        fout.write(line)
                        msg = "Saved %s bytes in file %s" % (size, filename)
                        sys.stdout.write(msg)
                        sys.stdout.flush()
                        amount_line += 1
            else:
                fout.write(str(row) + '\n')
                # amount_line = len(row)
        print("Sucessful file %s with %s bytes" % (filename, len(row)))
        # self.processing_finished = True
    except Exception as e:
        print(e)


def makeJsonManifest(number_of_files, destination):
    # making manifest
    urls = []
    urls.append({'url': 's3://' + destination, 'mandatory': True})
    for idx in range(number_of_files):
        if idx == 0:
            continue
        else:
            filename = destination + "." + str(idx)
        urls.append({'url': 's3://' + filename, 'mandatory': True})
    container = {}
    container['entries'] = urls
    return (str(json.dumps(container, sort_keys=False, indent=4)))
