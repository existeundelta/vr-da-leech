# Constant
def QUERY_LIMIT():
    return "10000"

def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(QUERY_LIMIT()).offset(offset):
            r = True
            yield elem
        offset += QUERY_LIMIT()
        if not r:
            break
