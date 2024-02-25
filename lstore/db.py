from lstore.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
        if not os.path.exists(path):
            os.mkdir(path)

        else:
            path = os.path.join(path, "metadata_file.pkl")
            file_open = open(path, 'rb')
            metadata = pickle.load(file_open)
            for table_name, table_metadata in metadata.items():
                table = self.create_new_table(*table_metadata[:3])
                self.set_table_attributes(table, table_metadata)
            file_open.close()

    def set_table_attributes(self, table, metadata):
        table.page_directory = metadata[3]
            

    def close(self):
        metadata = {}
        m_file_open = open("metadata_file.pkl", 'wb')
        pickle.dump(self, m_file_open)
        m_file_open.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
