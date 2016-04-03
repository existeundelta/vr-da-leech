import inspect
import sys
from mylibs.tools import formatROW
import blist


def saveLocalFile(row, filename, formatString=False, limit=0):
    print("Salving to local file")
    try:
        amount_line = 0
        size = 0
        fout = open(filename, 'w')
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
                    if limit > amount_line:
                        break
                    amount_line += 1
        else:
            size = len(row)
            fout.write(str(row) + '\n')
            # amount_line = len(row)
        print("Sucessful file %s with %s bytes" % (filename, size))
    # self.processing_finished = True
        return True
    except Exception as e:
        return False
        print(e)


def cleanLocal():
    pass
