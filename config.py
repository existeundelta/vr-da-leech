aws = dict(
    acceskey='AKIAJSM7XAJXT3XF7YKA'
    ,secretkey='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'
    ,redshift=dict(
        endpoint='data-events.cmgmyz3fii98.us-east-1.redshift.amazonaws.com'
        ,port='5439'
        ,login='pdi'
        ,password='Password1'
        ,database='analytics'
        ,schema='public'
    ),
)

source = dict(
    source_engine='postgresql' #mysql or others...
    ,endpoint='db-analytics.vivareal.com' #db-analytics.vivareal.com
    ,login='pdi'
    ,password='password'
    ,database='analytics'
    ,schema='dw'
    ,tables=dict(
        custom_tables='dw_listings_debug'
        ,exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='vr-datanalytics-spool' #'/Users/smaniotto/redshift',
    ,split_size=0 # bytes em lead 700000000 1.613.539
    ,resultset_size=0
    ,method='s3' # S3 or Local
    ,delimiter=';'
    ,thread='0'
)