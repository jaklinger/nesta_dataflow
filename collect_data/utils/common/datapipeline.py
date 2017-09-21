'''
datapipeline

'''

import logging

from utils.common.dataexport import write_json
from utils.common.googledrive import save_to_drive
import os

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import insert as sql_insert
from sqlalchemy.sql.expression import text as sql_text
from sqlalchemy.schema import Table
from sqlalchemy.schema import MetaData

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.sql.expression import func as sql_func

'''
'''
class DataPipeline:

    def __init__(self,config,drivername='mysql+pymysql'):
        '''
        '''
        self.drive_params = config["GoogleDrive"]
        
        params = config["parameters"]
        required_fields = ["table_name","input_db","output_db",
                           "src","description","github"]
        
        # Collect the required fields together
        self.metainfo = {}
        for field in required_fields:
            if field in config["parameters"]:
                self.metainfo[field] = config["parameters"][field]
            elif field in config["DEFAULT"]:
                self.metainfo[field] = config["DEFAULT"][field]
            else:
                raise RuntimeError("Could not find field "+field+" in configuration.")
        
        # Connect to the database
        output_db = self.metainfo["output_db"]
        db_cnf = URL(drivername=drivername,
                     query={'read_default_file':config["DEFAULT"][output_db]})
        self.engine = create_engine(name_or_url=db_cnf)
        self.conn = self.engine.connect()
        self.metadata = MetaData(bind=self.engine)
        
    def __enter__(self):
        '''Dummy method for with'''
        return self

    def __exit__(self, _type, _value, _traceback):
        """ 
        """        
        # Don't write metadata if there's an error
        if _type is None:
            self.update_metadata_db()
            # Write the main table and metadata to Drive
            for table_name in [self.metainfo["table_name"],"metadata"]:
                file_path = write_json(self.conn,table_name,destination="/tmp/")
                save_to_drive(file_path,self.drive_params)
                os.remove(file_path)
                
        # Close the engine connection
        self.conn.close()

    def update_metadata_db(self):
        '''
        '''
        # Otherwise generate the metadata
        metadata = MetaData(bind=self.engine)
        soft_table = Table(sql_text('metadata'),metadata,
                           autoload=True,mysql_charset='utf8',)        
        
        # Get the source(s), to be looped over
        sources = self.metainfo.pop("src")
        if type(sources) is not list:
            sources = [sources]

        # Insert each row of metadata (one per source)        
        logging.info("\tWriting to table metadata")
        self.metainfo["timestamp_update"] = sql_func.current_timestamp()
        for src in sources:
            self.metainfo["src"] = src
            _insert = mysql_insert(soft_table,values=self.metainfo)            
            _insert = _insert.on_duplicate_key_update(**self.metainfo)
            self.conn.execute(_insert)

    def insert(self,values):
        '''
        '''
        table = Table(sql_text(self.metainfo["table_name"]),self.metadata,
                     autoload=True,mysql_charset='utf8',)
        _insert_stmt = sql_insert(table).prefix_with("IGNORE")
        self.conn.execute(_insert_stmt.values(**values))
        
 
