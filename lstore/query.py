from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.table.index.locate(self.table.key, primary_key)

        if len(rid) > 1:
            return False
        
        self.table.delete_record(list(rid)[0])
        return True

    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        rids = self.table.index.locate(self.table.key, columns[self.table.key])
        if rids != None:
            return False
        #returns rid for now
        schema_encoding = '0' * self.table.num_columns
        return self.table.insert_record(list(columns))
        

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        rids = self.table.index.locate(search_key_index, search_key)
        records = []
        for rid in rids:
            record = self.table.read_record(rid, 0)
            cols = []
            for i, p in enumerate(projected_columns_index):
                if p: cols.append(record[i+7])
            records.append(Record(rid, self.table.key, cols))
        return records

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids = self.table.index.locate(search_key_index, search_key)
        records = []
        for rid in rids:
            record = self.table.read_record(rid, relative_version)
            cols = []
            for i, p in enumerate(projected_columns_index):
                if p: cols.append(record[i+7])
            records.append(Record(rid, self.table.key, cols))
        return records

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        #assume primary key is immutable/actually idk
        rid = self.table.index.locate(self.table.key, primary_key)
        if len(rid) > 1:
            return False
        return self.table.update_record(list(columns), list(rid)[0])

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        records = []
        s = 0
        for rid in rids:
            record = self.table.read_record(rid, 0)
            s = s + record[aggregate_column_index+7]
        return s

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        records = []
        s = 0
        for rid in rids:
            record = self.table.read_record(rid, relative_version)
            s = s + record[aggregate_column_index+7]
        return s

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
