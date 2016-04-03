import inspect
import sys

import blist


def saveLocalFile(row, filename):
    print("Salving to local file")
    try:
        amount_line = 0
        fout = open(filename, 'w')
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
    except IOError as e:
        print("Error on save %s in disk: %s" % (filename, e))


def cleanLocal():
    pass
