import itertools
import json
import sys
import threading
import time

import boto
from smart_open import *

import config


# http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html
# http://blogs.aws.amazon.com/bigdata/post/Tx2ANLN1PGELDJU/Best-Practices-for-Micro-Batch-Loading-on-Amazon-Redshift

class StreamingFile():
    resultset = None
    aws_access_key_id = config.aws['acceskey']
    aws_secret_access_key = config.aws['secretkey']
    cfg_split_size = config.streaming['split_size']
    cfg_resultset_size = config.streaming['resultset_size']
    cfg_delimiter = config.streaming['delimiter']
    cfg_method = config.streaming['method']
    cfg_folder_bucket = config.streaming['folder_or_bucket']

    destination = None
    delimiter = None
    processing_finished = False

    def __init__(self, resultset, filename):
        self.resultset = resultset
        self.destination = filename

    def makeJsonManifest(self, number_of_files):
        # making manifest
        urls = []
        urls.append({'url': 's3://' + self.destination, 'mandatory': True})
        for idx in range(number_of_files):
            filename = self.destination + "." + str(idx)
            urls.append({'url': 's3://' + filename, 'mandatory': True})
        container = {}
        container['entries'] = urls
        return (str(json.dumps(container, sort_keys=False, indent=4)))

    def delimiterFile(self, row):
        line_rebuild = ''
        row_len = len(row) - 1
        itemNull = False
        for idx, item in enumerate(row):
            # print("column: %s, item: %s" % (idx,item))

            # if idx == 0 and item == 222:
            #    print("Debug - cheguei")
            itemNull == False
            if (item == None):
                item = '\\N'
            if (item == '') or (item == ' '):
                #  NULL AS '\\N'
                item = ''
                itemNull = True

            line_rebuild = line_rebuild + str(item).replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            if idx < row_len:
                line_rebuild = line_rebuild + self.cfg_delimiter
            else:
                if (itemNull == True):
                    line_rebuild = line_rebuild + '\\N'
        # print(line_rebuild)
        return (line_rebuild)

    def animate(self, msg, extrainfo):
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if self.processing_finished:
                break
            sys.stdout.write('\r%s %s  -  %s' % (msg, c, extrainfo))
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write('\rDone!     ')

    def cleanS3(self, filename=None):
        self.processing_finished = False
        bucket = boto.connect_s3(self.aws_access_key_id, self.aws_secret_access_key).get_bucket(self.cfg_folder_bucket)
        for key, content in s3_iter_bucket(bucket, accept_key=lambda key: key.startswith(filename)):
            t = threading.Thread(target=self.animate("Deleting  S3 files: ", str(key) + " size: " + len(content)))
            t.start()
            bucket.delete(key)
        self.processing_finished = True

    def cleanFolder(self, filename):
        if self.cfg_method == 's3':
            self.cleanS3(filename)


    def savesS3(self, row, filename):
        # uri = `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`
        uri = "s3://%s:%s@%s" % (self.aws_access_key_id, self.aws_secret_access_key, filename)
        print("Salving data on bucket %s" % filename)
        try:
            amount_line = 0
            # Processing animation thread
            self.processing_finished = False
            t = threading.Thread(target=self.animate("Saving in S3: ", str(amount_line) + " rows saved"))
            t.start()
            with smart_open.smart_open(uri, 'wb') as fout:
                if type(row) is list or type(row) is tuple:
                    for line in row:
                        amount_line += 1
                        fout.write(line + '\n')
                else:
                    fout.write(str(row) + '\n')
                    # amount_line = len(row)
            print("Sucessful file %s with %s bytes" % (filename, len(row)))
            self.processing_finished = True
        except Exception as e:
            print(e)

    def saveLocalFile(self, row, filename):
        print("Salving to local file")
        try:
            amount_line = 0
            f = open(filename, 'w')
            for line in row:
                f.write(str(line) + '\n')
            print("Sucessful file %s with %s lines" % (filename, str(amount_line)))
        except Exception as e:
            print(e)

    def save(self):
        rows = []
        file_index = 0
        row_size = 0
        resultset_line = 0
        filename = self.destination
        for row in self.resultset:
            # converting database row to delimited text row
            row = self.delimiterFile(row)

            # If i want limit into number of sources row
            resultset_line += 1
            if (self.cfg_resultset_size != 0) and (resultset_line > self.cfg_resultset_size):
                print("Resulset limited by resultset_size... Exiting")
                break

            # Differnt of last step; I want control the number of line per file (Split streaming)
            # self.cfg_split_zise = without split
            if (self.cfg_split_size == 0):
                rows.append((row))
                row_size = row_size + 1
            else:
                if row_size < self.cfg_split_size:
                    rows.append((row))
                    row_size = row_size + 1
                    print("Export line: %s" % str(row_size))
                else:
                    if file_index > 0:
                        filename = self.destination + "." + str(file_index)
                    if self.cfg_method == 's3':
                        print("Save in S3")
                        self.savesS3(rows, filename)
                    elif self.cfg_method == 'local':
                        print("Save in local")
                        self.saveLocalFile(rows, filename)
                    row_size = 0
                    file_index += 1
                    rows.clear()

        # If my rows list is not empty, so i need streaming to new file with the final data.
        if len(rows) > 0:
            print("salva o resto...  %s linhas" % len(rows))
            if file_index > 0:
                file_index += 1
                filename = self.destination + "." + str(file_index)
            if self.cfg_method == 's3':
                print("Streaming to AWS S3")
                self.savesS3(rows, filename)
            elif self.cfg_method == 'local':
                print("Streaming to Local")
                self.saveLocalFile(rows, filename)

        rows.clear()
        # Make Manifest File
        manifest_file = self.destination + '.manifest'
        json = []

        self.savesS3(self.makeJsonManifest(file_index), manifest_file)
        # def makeManifest(self, filename):

