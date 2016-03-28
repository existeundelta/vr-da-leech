# http://docs.sqlalchemy.org/en/latest/core/compiler.html
from sqlalchemy import create_engine, Table, Column, MetaData, Text

import config


class RDS:
    rds_port = config.aws['redshift']['port']
    if rds_port == None:
        rds_port = 5349

    rds_endpoint = config.aws['redshift']['endpoint'] + ':' + config.aws['redshift']['port']
    rds_login = config.aws['redshift']['login']
    rds_password = config.aws['redshift']['password']
    rds_database = config.aws['redshift']['database']
    rds_schema = config.aws['redshift']['schema']
    rds_engine = None
    rds_metadata = None

    def __init__(self):
        uri_engine = 'postgresql+psycopg2://%s:%s@%s/%s' % (
            self.rds_login, self.rds_password, self.rds_endpoint, self.rds_database)
        print("Connectiong on RedShift: %s" % uri_engine)
        try:
            self.rds_engine = create_engine(uri_engine, echo=False)
            self.rds_metadata = MetaData(bind=self.rds_engine)
        except Exception as e:
            print("Error on connect rds : %s" % e)

    def cloneTable(self, table_name, source_metada):

        srcTable = Table(table_name, source_metada)
        destTable = Table(table_name, self.rds_metadata)

        # Drop if exist
        try:
            destTable.drop(self.rds_engine)
        except:
            print("Table not exist to drop")

        # copy schema and create newTable from oldTable
        for column in srcTable.columns:
            # print(column)
            if "varchar" in str(column.type).lower() or "json" in str(column.type).lower():
                custom_column = Column(str(column), Text)
                destTable.append_column(custom_column)
            else:
                destTable.append_column(column.copy())
        destTable.create(self.rds_engine)
