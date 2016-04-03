aws = dict(
    acceskey='AKIAJSM7XAJXT3XF7YKA',
    secretkey='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',
    redshift=dict(
        endpoint='10.110.3.39',
        port='5439',
        login='pdi',
        password='Password1',
        database='analytics',
        schema='public,'
    ),
)

source = dict(
    source_engine='postgresql', #mysql or others...
    endpoint='127.0.0.1', #db-analytics.vivareal.com
    login='pdi',
    password='password',
    database='analytics',
    schema='dw',
    tables=dict(
        custom_tables='dw_listings_debug',
        exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='/Users/smaniotto/redshift', #'vr-datanalytics-spool', #'/Users/smaniotto/redshift', #'vr-datanalytics-spool', # #
    split_size=0,  # 1M em lead
    resultset_size=0,
    method='local',  # S3 or Local
    delimiter=';',
    thread='',
)