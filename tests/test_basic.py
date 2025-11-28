
import unittest
import sqlite3


class TestBasic(unittest.TestCase):
    def test_basic(self):
        db = sqlite3.connect(":memory:")
        db.enable_load_extension(True)

        db.load_extension("./avro")
        db.execute("create virtual table some_avro using avro('twitter.avro')")
        cur = db.execute("select username, tweet, timestamp from some_avro")
        rows = cur.fetchall()
        assert rows == [
            ("miguno",  "Rock: Nerf paper, scissors is fine.", 1366150681),
            ("BlizzardCS",  "Works as intended.  Terran is IMBA.", 1366154481)
        ]

        db.close()


if __name__ == '__main__':
    unittest.main()
