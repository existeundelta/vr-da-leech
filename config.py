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

source = dict(
    source_engine='postgresql',
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
    split_size=500000,  # 500k em lead
    resultset_size=0,
    method='s3',
    delimiter=';',
    thread='3',
)
