#btree with object key and object val, maybe change to more specific stuff later
#from https://btrees.readthedocs.io/en/latest/index.html
import BTrees.OOBTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class MyBTree(BTrees.OOBTree.BTree):

    max_leaf_size = 50000
    max_internal_size = 10000


class Index:
    def __init__(self, table):
        # One index for each column for fast search. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns
        self.updated = [0] * table.num_columns
        #creates index for key columnu
        #create index for all
        for i in range(table.num_columns):
            self.create_index(i)
        #if value got updated need to reindex
        self.updated = [0] * table.num_columns
        self.index_lock_tid = None
        pass
    def get_index_lock(self, tid):
        if tid != self.index_lock_tid and self.index_lock_tid != None:
            return False
        return True
    def acquire_index_lock(self, tid):
        if tid != self.index_lock_tid and self.index_lock_tid != None:
            return False
        self.index_lock_tid = tid
        return True
    def release_index_lock(self, tid):
        self.index_lock_tid = None
        return True

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        self.update_index(column)
        if self.indices[column] is None:
            self.drop_index(column)
            self.create_index(column)
            self.updated[column] = 0
        return self.indices[column].get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        self.update_index(column)
        ridlists = self.indices[column].values(min = begin, max = end)
        rv = []
        for ridlist in ridlists:
            rv.extend(ridlist)
        return rv
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.drop_index(column_number)
        self.indices[column_number] = MyBTree()
        for rid in self.table.baserids:
            val = self.table.read_record(rid, 0)[column_number+7]
            self.addToIndex(column_number, val, rid)

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
            self.drop_index(column_number)
            self.create_index(column_number)
            self.updated[column_number] = 0

    def has_key(self, key):
        if key in self.indices[self.table.key]:
            return True
        else:
            return False
