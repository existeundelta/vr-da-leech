aws = dict(
    acceskey="AKIAJSM7XAJXT3XF7YKA",
    secretkey="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",
    redshift=dict(
        endpoint='data-events.cmgmyz3fii98.us-east-1.redshift.amazonaws.com',
        port='5439',
        login='awsuser',
        password='V1vivareal',
        database='analytics',
        schema='public,'
    ),
)

source = dict(
    source_engine='postgresql', #mysql or others...
    endpoint='db-analytics.vivareal.com',
    login='pdi',
    password='password',
    database='analytics',
    schema='dw',
    tables=dict(
        custom_tables='',
        exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='vr-datanalytics-spool',
    split_size=1000000,  # 1M em lead
    resultset_size=0,
    method='s3',  # S3 or Local
    delimiter=';',
    thread='2',
)