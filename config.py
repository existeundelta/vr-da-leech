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
    ,endpoint='127.0.0.1' #db-analytics.vivareal.com
    ,login='pdi'
    ,password='password'
    ,database='analytics'
    ,schema='dw'
    ,tables=dict(
        custom_tables='dw_inmueble,dw_inmueble_eliminado,dw_ubicacion,dw_suscripcion,dw_suahouse,dw_responsys_programa,dw_responsys_bounce,dw_responsys_click,dw_responsys_complaint,dw_responsys_programa,dw_responsys_sent,dw_responsys_skipped'
        ,exclude_tables='',
    )
)

streaming = dict(
    folder_or_bucket='vr-datanalytics-spool' #'/Users/smaniotto/redshift',
    ,split_size=0 # 1M em lead
    ,resultset_size=0
    ,method='s3' # S3 or Local
    ,delimiter=';'
    ,thread=''
)