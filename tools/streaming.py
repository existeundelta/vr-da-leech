import re

from blist import blist
from boto.exception import *
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
    cfg_streaming_thread = config.streaming['thread']

    destination = None
    delimiter = None
    processing_finished = False

    def makeJsonManifest(self, number_of_files):
        # making manifest
        urls = []
        urls.append({'url': 's3://' + self.destination, 'mandatory': True})
        for idx in range(number_of_files):
            if idx == 0:
                continue
            else:
                filename = self.destination + "." + str(idx)
            urls.append({'url': 's3://' + filename, 'mandatory': True})
        container = {}
        container['entries'] = urls
        return (str(json.dumps(container, sort_keys=False, indent=4)))

    def formatROW(self, row):
        line_rebuild = ''
        row_len = len(row) - 1  # -1 is necessery by if idx < row_len
        for idx, item in enumerate(row):
            if (item == None) or (item == ''):
                item = ''
            elif not str(item).isdigit():
                # Remove extra space and truncate if more then limit supported by varchar in RedShift
                item = re.sub("\s\s+", " ", str(item))
                item = (str(item)[:65517] + ' (...truncated...)') if len(str(item)) > 65535 else item
                # Add quote in String
                item = str(item).replace('"', "'")
                item = '"{}"'.format(item)
            line_rebuild = line_rebuild + str(item).replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            if idx < row_len:
                line_rebuild = line_rebuild + self.cfg_delimiter
        return (line_rebuild)

    def cleanS3(self, filename=None):
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

    def cleanS3ALL(self):
        bucket = boto.connect_s3(self.aws_access_key_id, self.aws_secret_access_key).get_bucket(
            self.cfg_folder_bucket)
        bucketListResultSet = bucket.list()
        for key in bucketListResultSet:
            bucket.delete_keys(key.name)
            print("%s deleted..." % key.name)

    def cleanLocal(self, filename):
        pass

    def cleanFolder(self, filename):
        if self.cfg_method == 's3':
            self.cleanS3(filename)
        if self.cfg_method == 'local':
            self.cleanLocal(filename)

    def cleanALL(self):
        if self.cfg_method == 's3':
            self.cleanS3ALL()

    def savesS3(self, row, filename):
        # uri = `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`
        uri = "s3://%s:%s@%s" % (self.aws_access_key_id, self.aws_secret_access_key, filename)
        print("Salving data on bucket %s" % filename)
        try:
            amount_line = 0
            with smart_open(uri, 'wb') as fout:
                if type(row) is list or type(row) is tuple or type(row) is blist:
                    for line in row:
                        if not line == None:
                            fout.write(line + '\n')
                            amount_line += 1
                else:
                    fout.write(str(row) + '\n')
                    # amount_line = len(row)
            print("Sucessful file %s with %s bytes" % (filename, len(row)))
            #self.processing_finished = True
        except Exception as e:
            print(e)

    def saveLocalFile(self, row, filename):
        print("Salving to local file")
        try:
            amount_line = 0
            f = open(filename, 'w')
            if type(row) is list or type(row) is tuple or type(row) is blist:
                for line in row:
                    f.write(str(line) + '\n')
            else:
                f.write(str(row) + '\n')
            print("Sucessful file %s with %s lines" % (filename, str(amount_line)))
        except Exception as e:
            print(e)

    def save(self, resultset, filename):
        self.resultset = resultset
        self.destination = filename

        rows = blist()
        file_index = 0
        row_size = 0
        resultset_line = 0
        filename = self.destination
        saved = False

        for row in self.resultset:
            saved = True
            try:
                # converting database row to delimited text row
                row = self.formatROW(row)
            except Exception as e:
                print("Error on format row: %s" % e)
            # If i want limit into number of sources row
            resultset_line += 1
            if (self.cfg_resultset_size != 0) and (resultset_line > self.cfg_resultset_size):
                print("")
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
                    msg = "\r -> Exporting to file %s - line number: %s" % (self.destination, str(row_size))
                    sys.stdout.write(msg)
                    sys.stdout.flush()
                else:
                    if file_index > 0:
                        filename = self.destination + "." + str(file_index)
                    if self.cfg_method.lower() == 's3':
                        print("Saving in S3....")
                        self.savesS3(rows, filename)
                    elif self.cfg_method.lower() == 'local':
                        print("Saving in local...")
                        self.saveLocalFile(rows, filename)
                    row_size = 0
                    file_index += 1
                    rows.clear()

        # If my rows list is not empty, so i need streaming to new file with the final data.
        if len(rows) > 0:
            print("")
            print("salva o resto...  %s linhas" % len(rows))
            if file_index > 0:
                filename = self.destination + "." + str(file_index)
            if self.cfg_method.lower() == 's3':
                print("Streaming to AWS S3")
                self.savesS3(rows, filename)
            elif self.cfg_method.lower() == 'local':
                print("Streaming to Local")
                self.saveLocalFile(rows, filename)

        # rows.clear()
        # Make Manifest File
        manifest_file = self.destination + '.manifest'
        if self.cfg_method.lower() == 's3':
            self.savesS3(self.makeJsonManifest(file_index), manifest_file)
        if self.cfg_method.lower() == 'local':
            self.saveLocalFile(self.makeJsonManifest(file_index), manifest_file)

        del rows
        return saved
