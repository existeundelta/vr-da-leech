import threading

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
# internal packages
from mylibs.streaming import *
from mylibs.tools import *
from mylibs.aws.redhisft import *


class Main:
    source_endpoint = config.source['endpoint']
    source_login = config.source['login']
    source_password = config.source['password']
    source_database = config.source['database']
    source_schema = config.source['schema']
    dsource_tables = config.source['tables']
    source_engine = config.source['source_engine']
    method = config.streaming['method']
    delimiter = config.streaming['delimiter']
    destination = config.streaming['folder_or_bucket']
    cfg_thread_number = config.streaming['thread']
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
        ## get all the results in a list of tuples
        exported = False
        try:
            # configure Session class with desired options
            Session = sessionmaker()
            Session.configure(bind=self.engine)
            session = Session()
            filename = self.destination + '/' + table + '.txt'
            streaming = StreamingFile()
            print("Streaming resulset to filename %s" % filename)
            exported = streaming.save(page_query(session.query(t)), filename)
        except (SQLAlchemyError, Exception) as e:
            print(e)
        finally:
            session.close()
            return exported

    def export2RedShift(self, table):
        try:
            manifest_file = "%s/%s.txt.manifest" % (self.destination, table)
            redshift = RedShift()
            redshift.cloneTable(table, self.metadata)
            if self.exportData(table):
                print("Importing...")
                redshift.importS3(table, manifest_file)
            else:
                print("Cannot export export data from table %s " % table)
        except (SQLAlchemyError, Exception) as e:
            print("Erro na importação RDS para S3: %s" % e)

    def run(self):
        try:
            print("Please waiting... Analyzing source database... please wait...")
            ## AutoMap
            self.metadata.reflect(self.engine)  # get columns from existing table
            Base = automap_base(bind=self.engine, metadata=self.metadata)
            Base.prepare(self.engine, reflect=True)
            MetaTable = Base.metadata.tables

            thread_list = []
            ## From all tables found in database
            for table in MetaTable.keys():
                if '.' in table:
                    table = str(table).split('.')[1]

                if len(config.source['tables']['exclude_tables']) > 0:
                    if exactyMatchList(config.source['tables']['exclude_tables'], table):
                        continue

                ## From config.py custom_tables
                if len(config.source['tables']['custom_tables']) > 0:
                    if not exactyMatchList(config.source['tables']['custom_tables'], table):
                        continue

                print("Preparing thread to table %s " % table)
                t = threading.Thread(target=self.export2RedShift, name='thread-' + table, args=(table,))
                thread_list.append(t)

            thread_control(thread_list, self.cfg_thread_number)
            print("Finish...")

        except (SQLAlchemyError, Exception) as e:
            print("Error: %s" % e)


if __name__ == "__main__":
    import sys

    cmdargs = str(sys.argv)


    app = Main()
    app.run()
    # streaming = StreamingFile()
    # streaming.cleanALL()
