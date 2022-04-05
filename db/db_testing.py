"""Tools to test databases and tables."""
import psycopg2 as pg

from db_config import config

db_params = config()


def db_testing(table_name, sql, db_params=None):
    """Test database connection and create table if it does not exist."""
    if not db_params:
        db_params = config()

    def test_db_connection():
        """Test if database connection is doable."""
        conn = None
        try:
            conn = pg.connect(**db_params)
            return conn
        except (Exception, pg.DatabaseError) as error:
            print(error)
        return False

    def test_table(table_name):
        """Test if table exists. Creates it if it does not."""
        with pg.connect(**db_params) as conn, conn.cursor() as curs:
            curs.execute(f"""
            SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """)
            table_exists: int = curs.fetchone()[0]
        if table_exists:
            return True
        return False

    def create_table(sql):
        """Create a table with a given SQL."""
        with pg.connect(**db_params) as conn, conn.cursor() as curs:
            curs.execute(sql)
            # commit the changes
            conn.commit()

    test_db_connection()
    if not test_table(table_name):
        create_table(sql)


if __name__ == "__main__":
    # This won't be run when imported
    pass
