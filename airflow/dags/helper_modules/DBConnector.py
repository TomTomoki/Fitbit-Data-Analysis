import psycopg2

class DBConnector():
    def __init__(self, db_user, db_password, db_name, host) -> None:
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.host = host

        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password, host=self.host)
        self.cur = self.conn.cursor()

    def close_connection(self):
        self.cur.close()
        self.conn.close()

    def execute_query(self, query):
        self.cur.execute(query)
        self.conn.commit()
