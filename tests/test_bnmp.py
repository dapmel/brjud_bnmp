"""BNMP tester."""
from datetime import datetime
import os
import psycopg2 as pg
import pytest
import random
import yaml

from db.db_config import config
from db.db_testing import DBTester
import BNMP

with open("utils/config.yml") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

# Parameters of the test database
db_params = cfg["testing"]["db_params"]


class TestDBUtils:
    """Test database utilities."""

    def test_config_file_exception(self):
        """Test validation of database configuration file."""
        test_data: dict = {"db_params":
                           {'host': 'localhost', 'database': 'jusdata_test',
                            'user': 'postgres'}}
        filename: str = "test_params.yml"
        path_with_file = f"db/{filename}"
        with open(path_with_file, "w") as outfile:
            yaml.dump(test_data, outfile, default_flow_style=False)

        # Check test file creation
        assert os.path.isfile(path_with_file)

        with pytest.raises(Exception) as exc_info:
            config(filename)
        assert exc_info.value.args[0] == \
            f"Section 'password' not found in '{filename}'"

        os.remove(path_with_file)
        # Check test file deletion
        assert os.path.isfile(path_with_file) is False

    def test_config_file_integrity(self):
        """Test keys of validated database configuration file."""
        params = config()
        for key in ["host", "database", "user", "password"]:
            assert key in params

    def test_dbtester(self):
        """Check if DBTester is effectively creating database and table."""
        self.db_params = cfg["testing"]["db_params"]

        # This call will create a table and a database if any does not exist
        DBTester("bnmp", cfg["sql"]["create"], self.db_params)

        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            # Assert everything was deleted
            curs.execute(cfg["testing"]["sql"]["find_table"], ("bnmp",))
            assert curs.fetchone()[0] == 1


class TestBNMP:
    """Test BNMP scrapers."""

    def reset_db(self):
        """Reset database."""
        assert pg.connect(**db_params)

        with pg.connect(**db_params) as conn, conn.cursor() as curs:
            # Drop all existing tables
            curs.execute(cfg["testing"]["sql"]["drop_all"])
            conn.commit()

        DBTester("bnmp", cfg["sql"]["create"], db_params)

    def test_bulk(self):
        """Test bulk scraper."""
        self.reset_db()
        scraper = BNMP.BulkScraper(db_params)
        # Manually changing test range
        scraper.states = range(1, 2)
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

    def test_details(self):
        """Test detail scraper."""
        self.reset_db()
        test_row = (176449921, 1, '00022746620198010001',
                    '0002274662019801000101000403', datetime(
                        2021, 3, 3).date(),
                    datetime(2022, 3, 3).date(), datetime(2022, 3, 3).date())
        with pg.connect(**db_params) as conn, conn.cursor() as curs:
            curs.execute(cfg["sql"]["insert"], test_row)
            conn.commit()

            scraper = BNMP.DetailsScraper(db_params)
            status = scraper.start()
            assert status

            curs.execute("SELECT * FROM bnmp;")
            data_rows = curs.fetchall()

            # Check if JSON field is filled and has correct format
            assert data_rows[0][-1] is not None
            assert isinstance(data_rows[0][-1], dict)
