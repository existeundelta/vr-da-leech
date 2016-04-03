def QUERY_LIMIT():  # Constant
    return int(100000)

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


def thread_control(thread_list=[], cfg_thread_number=1):
    import time
    threads_running = []
    amount_threads_running = 0
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
