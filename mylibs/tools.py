import config

def QUERY_LIMIT():  # Constant
    return int(30000)

def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(int(QUERY_LIMIT())).offset(offset):
            r = True
            yield elem
        offset += int(QUERY_LIMIT())
        if not r:
            break


def exactyMatchList(list, word):
    match = False
    if ',' in list:
        for i in list.split(','):
            if i.lower().strip() == word.lower().strip():
                match = True
    elif list.lower().strip() == word.lower().strip():
                match = True
    return match


def isNumber(number):
    try:
        a = float(number)
        return True
    except:
        return False


def formatROW(row):
    cfg_delimiter = config.streaming['delimiter']
    import re
    line_rebuild = ''
    row_len = len(row) - 1  # -1 is necessery by if idx < row_len
    for idx, item in enumerate(row):
        if (item == None) or (item == ''):
            item = ''
        if not isNumber(str(item)):
            # Remove extra space and truncate if more then limit supported by varchar in RedShift
            item = re.sub(r'\s+', ' ', str(item)).strip()
            if len(str(item)) > 64000:
                item = (str(item)[:64000] + ' (...truncated...)')
            # Add quote in String
            item = str(item).replace('"', "'")
            item = '"{}"'.format(item)
        else:
            pass

        line_rebuild = line_rebuild + str(item).replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        if idx < row_len:
            line_rebuild = line_rebuild + cfg_delimiter
    return (line_rebuild)


def thread_control(thread_list=[], cfg_thread_number=1):
    import time
    threads_running = []
    amount_threads_running = 0
    try:
        if int(cfg_thread_number) == 0:
            cfg_thread_number = 1
        while True:
            if (len(thread_list)) > 0:
                if int(cfg_thread_number) > amount_threads_running:
                    for thread in thread_list:
                        thread.start()
                        amount_threads_running += 1
                        threads_running.append(thread)
                        if int(amount_threads_running) >= int(cfg_thread_number):
                            break
                else:
                    for running in threads_running:
                        if not running.isAlive():
                            amount_threads_running -= 1
                            thread_list.remove(running)
                            threads_running.remove(running)
                        else:
                            time.sleep(5)
            else:
                break
    except Exception as e:
        print("Erron on started threads...")


