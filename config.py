aws = dict(
    acceskey='AWSACCESS'
    ,secretkey='AWS:SECRET'
    ,redshift=dict(
        endpoint='destinantion endpoin'
        ,port='5439'
        ,login=''
        ,password=''
        ,database=''
        ,schema='public'
    ),
)

source = dict(
    source_engine='postgresql' #mysql or others...
    ,endpoint='source endpoint' 
    ,login=''
    ,password=''
    ,database=''
    ,schema='dw'
    ,tables=dict(
        custom_tables='dw_listings_debug'
        ,exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='vr-datanalytics-spool' 
    ,split_size=0 # bytes em lead 700000000 1.613.539
    ,resultset_size=0
    ,method='s3' # S3 or Local
    ,delimiter=';'
    ,thread='0'
)
