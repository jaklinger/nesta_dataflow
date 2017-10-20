from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text as sql_text
from sqlalchemy.sql.expression import select as sql_select
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table

def read_all_results(config,input_db,input_table):
    # DB parameters for Wikipedia qualification mapping
    input_db = config["parameters"][input_db]
    input_table = config["parameters"][input_table]
    # Read DB
    db_cnf = URL(drivername="mysql+pymysql",
                 query={'read_default_file':config["DEFAULT"][input_db]})
    engine = create_engine(name_or_url=db_cnf)
    conn = engine.connect()
    md = MetaData(engine, reflect=True)
    table = Table(input_table, md, autoload=True, autoload_with=engine)
    results = conn.execute(sql_select([table])).fetchall()
    return results
