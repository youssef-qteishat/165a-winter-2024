from lstore.table import Table
from lstore.bufferpool import Bufferpool
import os
import pickle

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()
        self.path = None

    def open(self, path):
        if not os.path.exists(path):
            self.path = path
            self.bufferpool.set_path(path)
            os.mkdir(path)

        else:
            self.path = path
            self.bufferpool.set_path(path)
            path = os.path.join(path, "metadata_file.pkl")
            file_open = open(path, 'rb')
            self.tables = pickle.load(file_open)
            file_open.close()
            

    def close(self):
        self.bufferpool.dump_pool()
        path = os.path.join(self.path, "metadata_file.pkl")
        m_file_open = open(path, 'wb')
        pickle.dump(self.tables, m_file_open)
        m_file_open.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table
    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if name == table.name:
                self.tables.remove(table)
                return True
        
        return False
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if name == table.name:
                return table
        
        return False
