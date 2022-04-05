"""BNMP tester."""
from datetime import datetime
import psycopg2 as pg
import random

from utils import consts
import BNMP

from db.db_testing import db_testing

# Parameters of the test database
db_params = {
    "host": "localhost",
    "database": "jusdata_test",
    "user": "postgres",
    "password": "postgres"
}

sql_drop_all_tables = """
    DO $$ DECLARE
    r RECORD;
    BEGIN
    FOR r IN (SELECT tablename FROM pg_tables
        WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    END $$;
"""


def reset_db():
    """Reset database."""
    assert pg.connect(**db_params)

    with pg.connect(**db_params) as conn, conn.cursor() as curs:
        # Drop all existing tables
        curs.execute(sql_drop_all_tables)
        conn.commit()

    db_testing("bnmp", consts.sql_bnmp_create_table, db_params)


def test_bulk():
    """Test bulk scraper."""
    reset_db()
    scraper = BNMP.BulkScraper(db_params)
    # Manually changing test range
    scraper.states_range = range(1, 2)
    status = scraper.start()
    # `start()` returns True on successes
    assert status

    with pg.connect(**db_params) as conn, conn.cursor() as curs:
        curs.execute("SELECT * FROM bnmp;")
        data_rows = curs.fetchall()
        # State 1 usually has more than 2k entries
        assert len(data_rows) > 2_000

        # Very superficial type checks
        random_row = random.choice(data_rows)
        assert isinstance(random_row[0], int)
        assert random_row[6] == datetime.now().date()


def test_details():
    """Test detail scraper."""
    reset_db()
    test_row = (176449921, 1, '00022746620198010001',
                '0002274662019801000101000403', datetime(2021, 3, 3).date(),
                datetime(2022, 3, 3).date(), datetime(2022, 3, 3).date())
    with pg.connect(**db_params) as conn, conn.cursor() as curs:
        curs.execute(consts.sql_insert_mandado, test_row)
        conn.commit()

        scraper = BNMP.DetailsScraper(db_params)
        status = scraper.start()
        assert status

        curs.execute("SELECT * FROM bnmp;")
        data_rows = curs.fetchall()
        # Check if JSON field is filled and has correct format
        assert data_rows[0][-1] is not None
        assert isinstance(data_rows[0][-1], dict)
