# Constant

def QUERY_LIMIT():
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
    else:
        for i in list:
            if i.lower().strip() == word.lower().strip():
                match = True
    return match
