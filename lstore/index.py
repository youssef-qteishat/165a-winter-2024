#btree with object key and object val, maybe change to more specific stuff later
#from https://btrees.readthedocs.io/en/latest/index.html
from BTrees.OOBTree import OOBTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:
    def __init__(self, table):
        # One index for each column for fast search. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns
        self.updated = [0] * table.num_columns
        #creates index for key columnu
        #self.create_index(table.key)
        #create index for all
        for i in range(table.num_columns):
            self.create_index(i)
        #if value got updated need to reindex
        self.updated = [0] * table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        self.update_index(column)
        #print(self.indices[column].get(value))
        return self.indices[column].get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        self.update_index(column)
        #print(list(self.indices[column].keys()))
        ridlists = self.indices[column].values(min = begin, max = end)
        rv = []
        for ridlist in ridlists:
            rv.extend(ridlist)
        return rv
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        #print(column_number)
        self.drop_index(column_number)
        self.indices[column_number] = OOBTree()
        #figure out after we get organization done
        for rid in self.table.baserids:
            val = self.table.read_record(rid)[column_number+4]
            self.addToIndex(column_number, val, rid)
        #print(list(self.indices[column_number].keys()))

    def addToIndex(self, column_number, val, rid):
        #don't do anything if index doesn't exist
        if(self.indices[column_number] == None):
            return False
        if self.indices[column_number].get(val) == None:
            self.indices[column_number][val] = {rid}
        else:
            self.indices[column_number][val].add(rid)
        return True
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        self.updated[column_number] = 0
    
    def update_index(self, column_number):
        if self.indices[column_number] is None or self.updated[column_number] == 1:
            #print("???")
            self.drop_index(column_number)
            self.create_index(column_number)
            self.updated[column_number] = 0
