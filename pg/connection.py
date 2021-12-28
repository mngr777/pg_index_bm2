import psycopg2

def connect(params):
    connection = psycopg2.connect(**params)
    connection.autocommit = True
    connection.set_client_encoding('UTF8')
    return connection

class Connection:
    def __init__(self, params, name = '<noname>'):
        self.connection = None
        self.name = name
        self.params = params
        self.reconnect()

    def reconnect(self):
        if self.connection:
            self.connection.close()
        self.connection = connect(self.params)

    def run(self, path):
        with open(path, 'r') as fd:
            self.execute(fd.read())

    def execute(self, query, params=()):
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

    def cursor(self):
        return self.connection.cursor()
