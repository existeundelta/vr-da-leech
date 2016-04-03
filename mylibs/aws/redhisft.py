# http://docs.sqlalchemy.org/en/latest/core/compiler.html
import sys
import time

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
    cfg_delimiter = ';'  # hard code - config.streaming['delimiter']
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
        except SQLAlchemyError:
            print()
            print("Ok... table not exists... cannot drop it")

        print("Creating table %s on RedShift" % table_name)
        for column in srcTable.columns:
            if ("varchar" in str(column.type).lower()) or ("text" in str(column.type).lower()) or (
                        "json" in str(column.type).lower() or "inte" in str(column.type).lower()):
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
                  "format delimiter '%s' timeformat 'auto' removequotes  manifest truncatecolumns maxerror as 1000;" % (
                      table, manifest_file, aws_accss_key_if, aws_secret_acces_key, delimiter)

        try:
            # conn = self.redshift_engine.connect()
            Session = sessionmaker(bind=self.redshift_engine)
            # text(strcopy)

            session = Session()
            print()
            print('-----')
            print("Runing COPY FROM manifest file %s " % manifest_file)
            time.sleep(5)  # Give time to really sync files with S3
            session.execute(strcopy)
            session.commit()
            sucess = "Cool... Table %s Imported with successful :) \n" % table
            print(sucess)
            print('-----')
            try:
                with open("redshift_import-sucess.log", "a") as import_log:
                    import_log.write(sucess)
            except IOError as e:
                print("Error on write in log file %s" % e)
        except (SQLAlchemyError, Exception) as e:
            error = "Error on LOAD manifest %s: %s" % (manifest_file, e)
            print(error)
            try:
                with open("redshift_import-error.log", "a") as error_log:
                    error_log.write(error)
            except IOError as e:
                print("Error on write in log file %s" % e)
        finally:
            session.close()
