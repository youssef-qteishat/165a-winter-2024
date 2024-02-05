from index import Index
from time import time
from range import Range

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.page_ranges = [Range(4 + self.num_columns, self.key)]
        self.last_rid = 0
        #self.index = Index(self)

    def insert_record(self, columns):
        """
        Add a new base record to the table with columnar values

        :param columns: []      #List containing column values for record
        """

        # if the page range does not have capacity create a new one
        if self.page_ranges[-1].has_capacity() != True:

            self.page_ranges.append(Range(4 + self.num_columns, self.key))

        # get the latest range
        page_range = self.page_ranges[-1]

        # get a new rid
        rid = self.get_new_rid()

        # load the colomn daat into a list
        column_values = [0, rid, int(1000000 * time()), 0] + columns

        # add the base record and remember its location
        base_page_number, offset = page_range.add_base_record(column_values)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [page_range_number, base_page_number, offset]})

        # return the rid to the caller (mostly used for testing right now)
        return rid

    def read_record(self, rid):

        # get the location from the page directory
        page_range_number, base_page_number, offset = self.page_directory.get(rid)

        # read the record from the location
        columns = self.page_ranges[page_range_number].read_record(base_page_number, offset)

        # return the column values to the caller (mostly used for testing right now)
        return columns


    def update_record(self, columns):
        pass

    def get_new_rid(self):

        # update and return the rid
        self.last_rid += 1
        return self.last_rid
    
    def __merge(self):
        #milestone2 we don't care lets goo
        print("merge is happening")
        pass
 
