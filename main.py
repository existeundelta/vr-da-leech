from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

# internal packages
from tools.streaming import *
from tools import ORMTools
from aws.rds.rds import *


class Main:
    source_endpoint = config.source['endpoint']
    source_login = config.source['login']
    source_password = config.source['password']
    source_database = config.source['database']
    source_schema = config.source['schema']
    source_tables = config.source['tables']
    source_engine = config.source['source_engine']
    method = config.streaming['method']
    delimiter = config.streaming['delimiter']
    destination = config.streaming['folder_or_bucket']
    engine = None

    def __init__(self):
        uri_engine = ''
        if 'postgres' in self.source_engine.lower():
            uri_engine = 'postgresql+psycopg2'
        elif 'mysql' in self.source_engine.lower():
            uri_engine = 'mysql+mysqldb'

        uri_engine = uri_engine + '://%s:%s@%s/%s' % (
            self.source_login, self.source_password, self.source_endpoint, self.source_database)
        self.engine = create_engine(uri_engine, echo=False)
        self.metadata = MetaData(bind=self.engine, schema=self.source_schema)

    def processRDS(self, table):
        t = Table(str(table), self.metadata, autoload=True, schema=self.source_schema)
        s = select([t])

        ## get all the results in a list of tuples
        conn = self.engine.connect()
        try:
            # configure Session class with desired options
            Session = sessionmaker()
            Session.configure(bind=self.engine)
            session = Session()

            rds = RDS()
            rds.cloneTable(str(table), self.metadata)

            filename = self.destination + '/' + table + '.txt'
            print("Streaming resulset to filename %s" % filename)
            streaming = StreamingFile(ORMTools.page_query(session.query(t)), filename)
            streaming.save()
        except (SQLAlchemyError, Exception) as e:
            print(e)
        finally:
            session.close()

    def run(self):
        try:
            conn = self.engine.connect()

            tables = config.source['tables']['custom_tables']
            if tables == '' or tables == None:
                print("Load Source tables...")
                ## AutoMap
                self.metadata.reflect(self.engine)  # get columns from existing table
                Base = automap_base(bind=self.engine, metadata=self.metadata)
                Base.prepare(self.engine, reflect=True)
                MetaTable = Base.metadata.tables

                for table in MetaTable.keys():
                    print(table)
                    if '.' in table:
                        table = str(table).split('.')[1]

                    if table in config.source['tables']['exclude_tables']:
                        continue

                    self.processRDS(table)
            else:
                for table in tables.split(','):
                    print("Processing custom tables... Table: %s" % table)
                    self.processRDS(table)
            conn.close()

        except (SQLAlchemyError, Exception) as e:
            print("Error: %s" % e)


## Implementar controle de menu..
if __name__ == "__main__":
    app = Main()
    app.run()
