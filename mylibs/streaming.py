import re

from blist import blist

from mylibs.Tools import *
from mylibs.aws.s3files import *
from mylibs.localfiles import *

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

    def formatROW(self, row):
        line_rebuild = ''
        row_len = len(row) - 1  # -1 is necessery by if idx < row_len
        for idx, item in enumerate(row):
            if (item == None) or (item == ''):
                item = ''
            if not isNumber(str(item)):
                # Remove extra space and truncate if more then limit supported by varchar in RedShift
                item = re.sub("\s\s+", " ", str(item))
                if len(str(item)) > 64000:
                    item = (str(item)[:64000] + ' (...truncated...)')
                # Add quote in String
                item = str(item).replace('"', "'")
                item = '"{}"'.format(item)
            else:
                pass

            line_rebuild = line_rebuild + str(item).replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            if idx < row_len:
                line_rebuild = line_rebuild + self.cfg_delimiter
        return (line_rebuild)

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
                if self.cfg_method.lower() == 's3':
                    print("Saving in S3....")
                    saveS3(row, filename)
                elif self.cfg_method.lower() == 'local':
                    print("Saving in local...")
                    saveLocalFile(row, filename)
                break
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
                        saveS3(rows, filename)
                    elif self.cfg_method.lower() == 'local':
                        print("Saving in local...")
                        saveLocalFile(rows, filename)
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
                saveS3(rows, filename)
            elif self.cfg_method.lower() == 'local':
                print("Streaming to Local")
                saveLocalFile(rows, filename)
        # rows.clear()
        # Make Manifest File
        manifest_file = self.destination + '.manifest'
        if self.cfg_method.lower() == 's3':
            saveS3(makeJsonManifest(file_index, self.destination), manifest_file)
        if self.cfg_method.lower() == 'local':
            saveLocalFile(makeJsonManifest(file_index, self.destination), manifest_file)

        del rows
        return saved
