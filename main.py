import threading

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
            print("Clean spool folder...")
            streaming.cleanFolder(table)
            print("Streaming resulset to filename %s" % filename)
            exported = streaming.save(ORMTools.page_query(session.query(t)), filename)
        except (SQLAlchemyError, Exception) as e:
            print(e)
        finally:
            session.close()
            return exported

    # def importRedShift(self, table):
    #    manifest_file = "%s/%s.txt.manifest" % (self.destination, table)
    #    try:
    #        redshift = RedShift()
    #        redshift.cloneTable(str(table), self.metadata)
    #        redshift.importS3(table, manifest_file)
    #    except (SQLAlchemyError, Exception) as e:
    #        print("Erro na importação RDS para S3: %s" % e)

    def export2RedShift(self, table):
        try:
            manifest_file = "%s/%s.txt.manifest" % (self.destination, table)
            redshift = RedShift()
            redshift.cloneTable(str(table), self.metadata)
            if self.exportData(table):
                redshift.importS3(table, manifest_file)
            else:
                print("Cannot export export data from table %s " % table)
        except (SQLAlchemyError, Exception) as e:
            print("Erro na importação RDS para S3: %s" % e)

    def run(self):
        try:
            tables = config.source['tables']['custom_tables']

            if tables == '' or tables == None:
                print("Load Source tables... please wait...")
                ## AutoMap
                # conn = self.engine.connect()
                self.metadata.reflect(self.engine)  # get columns from existing table
                Base = automap_base(bind=self.engine, metadata=self.metadata)
                Base.prepare(self.engine, reflect=True)
                MetaTable = Base.metadata.tables

                # I will working with threads here...
                thread_list = []
                if str(self.cfg_thread_number).isdigit() or (not self.cfg_thread_number == '') or (
                not self.cfg_thread_number == None):
                    if int(self.cfg_thread_number) > 0:
                        # Making list of threads with for all tables...
                        for table in MetaTable.keys():
                            if '.' in table:
                                table = str(table).split('.')[1]

                            if table in config.source['tables']['exclude_tables']:
                                continue

                            print("Preparing thread to table %s " % table)
                            # t = Thread(target=self.processAll, args=(table))
                            t = threading.Thread(target=self.export2RedShift, args=(table,))
                            thread_list.append(t)

                        # And now I will processing step by step in rule of cfg_thread_number...
                        threads_running = []
                        amount_threads_running = 0
                        for thread in thread_list:
                            if int(self.cfg_thread_number) > amount_threads_running:
                                print("Processing thread  %s in paralell " % amount_threads_running)
                                thread.start()
                                threads_running.append(thread)
                                amount_threads_running += 1
                            else:
                                for running in threads_running:
                                    if running.isAlive():
                                        print("Waiting thread %s finish" % amount_threads_running)
                                        running.join()
                                        amount_threads_running -= 1
                                    else:
                                        amount_threads_running -= 1
                                        continue
                                threads_running.clear()
                        exit(0)  # Happy end...

                # Without Threads...  Working in sequencial mode...
                for table in MetaTable.keys():
                    print("Prepara processing in table %s " % table)
                    if '.' in table:
                        table = str(table).split('.')[1]

                    if table in config.source['tables']['exclude_tables']:
                        continue
                    self.export2RedShift(table)

            else:
                for table in tables.split(','):
                    print("Processing custom tables... Table: %s" % table)
                    self.export2RedShift(table)
        except (SQLAlchemyError, Exception) as e:
            print("Error: %s" % e)


if __name__ == "__main__":
    app = Main()
    app.run()
    # streaming = StreamingFile()
    # streaming.cleanALL()
