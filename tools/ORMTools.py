query_limit = 10000


def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(query_limit).offset(offset):
            r = True
            yield elem
        offset += query_limit
        if not r:
            break

# for item in page_query(Session.query(Picture)):
#    print
#    item
