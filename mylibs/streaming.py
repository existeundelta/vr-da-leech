import re

from blist import blist

from mylibs.aws.s3files import *
from mylibs.localfiles import *
from mylibs.tools import *

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

    def __init__(self):
        pass


    def cleanFolder(self, filename):
        if self.cfg_method == 's3':
            cleanS3(filename)
        if self.cfg_method == 'local':
            cleanLocal(filename)

    def cleanALL(self):
        if self.cfg_method == 's3':
            cleanS3ALL()

    def save(self, resultset, filename):
        self.resultset = resultset
        self.destination = filename

        file_index = 0
        row_size = 0
        resultset_line = 0
        filename = self.destination
        saved = False
        try:
            rows = blist.blist()
        except Exception as e:
            print("Erro on create blist %s" % e)
            exit(1)

        if (self.cfg_split_size == 0):
            if self.cfg_method.lower() == 's3':
                print("Saving in S3....")
                saved = saveS3(resultset, filename, True, self.cfg_resultset_size)
            elif self.cfg_method.lower() == 'local':
                print("Saving in local...")
                saved = saveLocalFile(resultset, filename, True, self.cfg_resultset_size)
        else:
            # Split File
            for row in resultset:
                saved = True
                try:
                    # converting database row to delimited text row
                    row = formatROW(row)
                except Exception as e:
                    print("Error on format row: %s" % e)
                # Split Files in cfg_split_size
                if len(rows) < int(self.cfg_split_size):
                    rows.append((row))
                    row_size = row_size + 1
                    msg = "\r -> Joing to file %s - bytes: %s" % (self.destination, str(row_size))
                    sys.stdout.write(msg)
                    sys.stdout.flush()
                else:
                    if file_index > 0:
                        filename = self.destination + "." + str(file_index)
                    if self.cfg_method.lower() == 's3':
                        print("Saving in S3....")
                        saveS3(rows, filename, False, self.cfg_resultset_size)
                    elif self.cfg_method.lower() == 'local':
                        print("Saving in local...")
                        saveLocalFile(rows, filename, False, self.cfg_resultset_size)
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
                    saveS3(rows, filename, False, 0)
                elif self.cfg_method.lower() == 'local':
                    print("Streaming to Local")
                    saveLocalFile(rows, filename, False, 0)
            # rows.clear()
            saved = True
            del rows


        # Make Manifest File
        manifest_file = self.destination + '.manifest'
        if self.cfg_method.lower() == 's3':
            saveS3(makeJsonManifest(file_index, self.destination), manifest_file, False)
        if self.cfg_method.lower() == 'local':
            saveLocalFile(makeJsonManifest(file_index, self.destination), manifest_file, False)
        return saved
