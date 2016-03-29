# http://docs.sqlalchemy.org/en/latest/core/compiler.html
import sys

from sqlalchemy import create_engine, Table, Column, MetaData, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import config


class RedShift:
    redshift_port = config.aws['redshift']['port']
    if redshift_port == None:
        redshift_port = 5349

    redshift_endpoint = config.aws['redshift']['endpoint'] + ':' + config.aws['redshift']['port']
    redshift_login = config.aws['redshift']['login']
    redshift_password = config.aws['redshift']['password']
    redshift_database = config.aws['redshift']['database']
    redshift_schema = config.aws['redshift']['schema']
    # cfg_delimiter = config.streaming['delimiter']
    cfg_delimiter = ';'  # hard code
    redshift_engine = None
    redshift_metadata = None

    def __init__(self):
        uri_engine = 'postgresql+psycopg2://%s:%s@%s/%s' % (
            self.redshift_login, self.redshift_password, self.redshift_endpoint, self.redshift_database)
        print("Connectiong on RedShift: %s" % uri_engine)
        try:
            self.redshift_engine = create_engine(uri_engine, echo=False)
            self.redshift_metadata = MetaData(bind=self.redshift_engine)
        except Exception as e:
            print("Error on connect redshift : %s" % e)

    def cloneTable(self, table_name, source_metada):

        srcTable = Table(table_name, source_metada)
        destTable = Table(table_name, self.redshift_metadata)

        # Drop if exist
        try:
            print()
            print("Trying to drop table %s if exists" % table_name)
            destTable.drop(self.redshift_engine)
        except SQLAlchemyError as e:
            print()
            print("Ok... table not exists... cannot drop it")

        print("Creating table %s on RedShift" % table_name)
        for column in srcTable.columns:
            if (str(column) == "dw_blacklist.tipo"):
                print("cheguei...")
            if ("varchar" in str(column.type).lower()) or ("text" in str(column.type).lower()) or (
                "json" in str(column.type).lower()):
                msg = "\r ->Change column type  of %s from  %s to VARCHAR(65535)" % (str(column), str(column.type))
                sys.stdout.write(msg)
                sys.stdout.flush()
                custom_column = Column(str(column).split('.')[1], String(65535))
                destTable.append_column(custom_column)
            else:
                destTable.append_column(column.copy())
        destTable.create(self.redshift_engine)

    def importS3(self, table, manifest_file):
        aws_accss_key_if = config.aws['acceskey']
        aws_secret_acces_key = config.aws['secretkey']

        delimiter = self.cfg_delimiter
        if self.cfg_delimiter == '\t':
            delimiter = '\\t'


        strcopy = "copy %s from 's3://%s'  " \
                  "credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' " \
                  "format delimiter '%s' timeformat 'auto' removequotes  manifest;" % (
                      table, manifest_file, aws_accss_key_if, aws_secret_acces_key, delimiter)

        try:
            # conn = self.redshift_engine.connect()
            Session = sessionmaker(bind=self.redshift_engine)
            # text(strcopy)

            s = Session()
            print()
            print('-----')
            print("Runing COPY FROM manifest file %s " % manifest_file)
            # time.sleep(5)
            result = s.execute(strcopy)
            s.commit()
            # conn.execute(text(strcopy))
            # print(result)
            print("Cool... Imported with successful :)")
            print('-----')

        except (SQLAlchemyError, Exception) as e:
            print("Error on LOAD manifest %s: %s" % (manifest_file, e))
