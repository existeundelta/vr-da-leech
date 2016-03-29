from sqlalchemy import select
from sqlalchemy.ext.automap import automap_base

# internal packages
from tools.streaming import *
from tools import ORMTools
from aws.redshift.redhisft import *


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

    def exportData(self, table):
        t = Table(str(table), self.metadata, autoload=True, schema=self.source_schema)
        s = select([t])

        ## get all the results in a list of tuples
        conn = self.engine.connect()
        try:
            # configure Session class with desired options
            Session = sessionmaker()
            Session.configure(bind=self.engine)
            session = Session()
            filename = self.destination + '/' + table + '.txt'
            streaming = StreamingFile()
            print("Clean spool folder...")
            streaming.cleanFolder(table)
            print("Streaming resulset to filename %s" % filename)
            streaming.save(ORMTools.page_query(session.query(t)), filename)
        except (SQLAlchemyError, Exception) as e:
            print(e)
        finally:
            session.close()

    def importRedShift(self, table):
        manifest_file = ("%s/%s.txt.manifest") % (self.destination, table)
        try:
            redshift = RedShift()
            redshift.cloneTable(str(table), self.metadata)
            redshift.importS3(table, manifest_file)
        except (SQLAlchemyError, Exception) as e:
            print("Erro na importação RDS para S3: %s" % e)


    def run(self):
        try:
            conn = self.engine.connect()

            # print()
            # print("Clean all old export file")
            # old_files = StreamingFile()
            # old_files.cleanALL()

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

                    self.exportData(table)
                    self.importRedShift(table)
            else:
                for table in tables.split(','):
                    print("Processing custom tables... Table: %s" % table)
                    self.exportData(table)
                    self.importRedShift(table)
            conn.close()

        except (SQLAlchemyError, Exception) as e:
            print("Error: %s" % e)


## Implementar controle de menu..
if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    # logger = logging.getLogger(__name__)

    app = Main()
    app.run()
