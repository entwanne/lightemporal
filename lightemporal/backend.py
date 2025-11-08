import atexit
import sqlite3


class Backend:
    def __init__(self, db_path='lightemporal.db'):
        self.db_path = db_path
        self.db = None
        self.cursor = None

    def __enter__(self):
        self.db = sqlite3.connect(self.db_path)
        self.cursor = self.db.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS workflows(id, name, input, status)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS activity_results(workflow_id, name, input, output)')
        self.db.commit()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.cursor.close()
        self.db.close()

    def get_workflow(self, id: str):
        #self.cursor.execute('SELECT ')
        pass


backend_ctx = Backend()
DB = backend_ctx.__enter__()
atexit.register(backend_ctx.__exit__, None, None, None)
