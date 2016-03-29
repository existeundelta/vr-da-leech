aws = dict(
    acceskey="YourKey",
    secretkey="YouSecret",
    redshift=dict(
        endpoint='RedShiftEndPoint',
        port='5439',
        login='redshift_login',
        password='redshiftpass',
        database='dst_db',
        schema='public,'
    ),
)

source = dict(
    source_engine='postgresql',  # mysql or others...
    endpoint='source_url',
    login='source_login',
    password='source_password',
    database='source_database',
    schema='source_schema',
    tables=dict(
        custom_tables='',
        exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='bucketS3',
    split_size=500000,  # 500k em lead
    resultset_size=0,
    method='s3',
    delimiter=';',
    thread='3',
)
