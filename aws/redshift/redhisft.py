# http://docs.sqlalchemy.org/en/latest/core/compiler.html
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
    redshift_engine = None
    redshift_metadata = None

    def __init__(self):
        uri_engine = 'postgresql+psycopg2://%s:%s@%s/%s' % (
            self.redshift_login, self.redshift_password, self.redshift_endpoint, self.redshift_database)
        print("Connectiong on RedShift: %s" % uri_engine)
        try:
            self.redshift_engine = create_engine(uri_engine, echo=True)
            self.redshift_metadata = MetaData(bind=self.redshift_engine)
        except Exception as e:
            print("Error on connect redshift : %s" % e)

    def cloneTable(self, table_name, source_metada):

        srcTable = Table(table_name, source_metada)
        destTable = Table(table_name, self.redshift_metadata)

        # Drop if exist
        try:
            destTable.drop(self.redshift_engine)
        except:
            print("Table not exist to drop")

        # copy schema and create newTable from oldTable
        for column in srcTable.columns:
            print("Original: %s é do tipo: %s" % (str(column), str(column.type)))
            if (str(column) == "dw_blacklist.tipo"):
                print("cheguei...")
            if "varchar" in str(column.type).lower():  # if isinstance(column.type, types.DATETIME):
                custom_column = Column(str(column).split('.')[1], String(65535))
                destTable.append_column(custom_column)
            elif "text" in str(column.type).lower() or "json" in str(column.type).lower():
                custom_column = Column(str(column).split('.')[1], String(65535))
                destTable.append_column(custom_column)
            else:
                destTable.append_column(column.copy())
        destTable.create(self.redshift_engine)

    def importS3(self, table, manifest_file):
        aws_accss_key_if = config.aws['acceskey']
        aws_secret_acces_key = config.aws['secretkey']

        strcopy = "copy %s from 's3://%s'  " \
                  "credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' " \
                  "delimiter '\\t' manifest;" % (table, manifest_file, aws_accss_key_if, aws_secret_acces_key)

        try:
            # conn = self.redshift_engine.connect()
            Session = sessionmaker(bind=self.redshift_engine)
            # text(strcopy)

            s = Session()
            print("Runing COPY FROM manifest file %s " % manifest_file)
            # time.sleep(5)
            result = s.execute(strcopy)
            s.commit()
            # conn.execute(text(strcopy))
            print(result)

        except (SQLAlchemyError, Exception) as e:
            print("Error on LOAD manifest %s: %s" % (manifest_file, e))
