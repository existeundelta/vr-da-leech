import inspect

import blist
from boto.exception import *
from smart_open import *
from mylibs.tools import formatROW

import config

aws_access_key_id = config.aws['acceskey']
aws_secret_access_key = config.aws['secretkey']
cfg_folder_bucket = config.streaming['folder_or_bucket']
cfg_streaming_thread = config.streaming['thread']

def cleanS3(filename=None):
    try:
        bucket = boto.connect_s3(aws_access_key_id, aws_secret_access_key).get_bucket(cfg_folder_bucket)
        for key, content in s3_iter_bucket(bucket, accept_key=lambda key: key.startswith(filename),
                                           workers=int(cfg_streaming_thread)):
            msg = "\r -> Deleting S3 Files: %s " % str(key)
            sys.stdout.write(msg)
            sys.stdout.flush()
            key.delete()
        print("")
    except (BotoServerError, BotoClientError, S3ResponseError, Exception) as e:
        print("Error on delete %s: " % e)


def cleanS3ALL():
    bucket = boto.connect_s3(aws_access_key_id, aws_secret_access_key).get_bucket(cfg_folder_bucket)
    bucketListResultSet = bucket.list()
    for key in bucketListResultSet:
        bucket.delete_keys(key.name)
        print("%s deleted..." % key.name)


def saveS3(row, filename, formatString=False, limit=0):
    # uri = `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`
    uri = "s3://%s:%s@%s" % (aws_access_key_id, aws_secret_access_key, filename)
    print("Salving data on bucket %s" % filename)
    size = 0
    try:
        amount_line = 0
        print("")
        with smart_open(uri, 'wb') as fout:
            # OR RESULT SET...
            if type(row) is list or type(row) is tuple or type(row) is blist or inspect.isgenerator(row):
                for line in row:
                    if not line == None:
                        if formatString:
                            line = formatROW(line)
                        line = str(line) + '\n'
                        size += len(line)
                        fout.write(line)
                        msg = "\r Saved %s bytes in file %s" % (size, filename)
                        sys.stdout.write(msg)
                        sys.stdout.flush()
                        sys.stdout.write("\r                                                       ")
                        sys.stdout.flush()
                        if limit > amount_line:
                            break
                        amount_line += 1
            else:
                size = len(row)
                fout.write(str(row) + '\n')
                # amount_line = len(row)
        print("\n Sucessful file %s with %s bytes" % (filename, size))
        # self.processing_finished = True
        return True
    except Exception as e:
        return False
        print(e)


def makeJsonManifest(number_of_files=0, destination=''):
    # making manifest
    urls = []
    urls.append({'url': 's3://' + destination, 'mandatory': True})
    if number_of_files > 0:
        for idx in range(number_of_files):
            filename = destination + "." + str(idx)
            urls.append({'url': 's3://' + filename, 'mandatory': True})
    container = {}
    container['entries'] = urls
    return (str(json.dumps(container, sort_keys=False, indent=4)))
