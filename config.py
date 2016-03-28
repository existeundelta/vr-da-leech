aws = dict(
    region='us-east-1',
    region_endpoint='apigateway.us-east-1.amazonaws.com',
    acceskey="AKIAJSM7XAJXT3XF7YKA",
    secretkey="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",
    redshift=dict(
        endpoint='dw-redshift.vivareal.com',
        port='5439',
        login='pg_admin',
        password='c4p4er9mbi5yncR',
        database='analytics',
        schema='public,'
    ),
)

pgsql1 = dict(
    endpoint='dbslave.vivareal.com',
    login='vivareal',
    password='jp03e4S!',
    database='vivareal',
    schema='public',
    tables='all',
)

pgsql2 = dict(
    endpoint='db-analytics.vivareal.com',
    login='pdi',
    password='password',
    database='analytics',
    schema='dw',
    tables='dw_cuenta',
    tables_exclude='dw_listings, dw_inmuble'
)

source = dict(
    source_engine='postgresql',
    endpoint='127.0.0.1',
    login='pdi',
    password='password',
    database='analytics',
    schema='dw',
    tables=dict(
        custom_tables='dw_blacklist',
        exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='vr-datanalytics-spool',
    split_size=0,
    resultset_size=0,
    method='s3',
    delimiter='\t',
    thread='3',

)
