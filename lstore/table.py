from lstore.index import Index
from time import time
from lstore.range import Range

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
        self.baserids = []
        self.page_directory = {}
        self.page_ranges = [Range(4 + self.num_columns, self.key)]
        self.last_rid = 0
        self.index = Index(self)
        self.deletedrids = []

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
        tail_columns = [rid, rid, int(1000000 * time()), 0] + columns

        # add the base record and remember its location
        base_page_number, offset = page_range.add_base_record(tail_columns)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [page_range_number, base_page_number, offset]})
        self.baserids.append(rid)
        #add to index
        for i in range(len(columns)):
            self.index.addToIndex(i, columns[i], rid)

        # return the rid to the caller (mostly used for testing right now)
        return rid

    def read_record(self, rid, version):
        # get the location of the base page by its rid
        page_range_number, base_page_number, offset = self.page_directory.get(rid)
        base_record = self.page_ranges[page_range_number].read_base_record(base_page_number, offset)
        schema_encoding = base_record[SCHEMA_ENCODING_COLUMN]

        # get the indirection column and schema encoding
        tail_record_id = base_record[INDIRECTION_COLUMN]

        for i in range(version-1, 0):
            if tail_record_id == rid:
            # read the record from the location
                return base_record
            # get the location from the page directory
            page_range_number, base_page_number, offset = self.page_directory.get(tail_record_id)
            # get the tail record columns
            tail_columns = self.page_ranges[page_range_number].read_tail_record(base_page_number, offset)
            #use the schema encoding to reconstruct full record
            schema_encoding = tail_columns[SCHEMA_ENCODING_COLUMN]
            for i in range(4, len(tail_columns)):
                if (not (1 & (schema_encoding >> (len(tail_columns) - (i + 1))))):
                    tail_columns[i] = base_record[i]
            tail_record_id = tail_columns[INDIRECTION_COLUMN]

        return tail_columns

    def update_record(self, columns, baserid):

        # if the page range does not have capacity create a new one
        if self.page_ranges[-1].has_capacity() != True:
            self.page_ranges.append(Range(4 + self.num_columns, self.key))

        # get the latest range
        page_range = self.page_ranges[-1]
        #print(baserid)
        lastrecord = self.read_record(baserid, 0)
        lastrid = lastrecord[RID_COLUMN]
        # get the schema encoding
        binary_string = ''
        for value in columns:
            if value is not None:
                binary_string += '1'
            else:
                binary_string += '0'
        schema_encoding = int(binary_string, 2) | lastrecord[SCHEMA_ENCODING_COLUMN]

        # get a new rid
        rid = self.get_new_rid()
        # update the schema encoding and indirection of the base page
        basepagerange, basepagenum, offset = self.page_directory[baserid]
        self.page_ranges[basepagerange].change_indirection(basepagenum, offset, rid)
        self.page_ranges[basepagerange].change_schema_encoding(basepagenum, offset, schema_encoding)

        # load the colomn daat into a list
        tail_columns = [lastrid, rid, int(1000000 * time()), schema_encoding] + columns
        for i in range(4, len(tail_columns)):
            if (1 & (schema_encoding >> (len(tail_columns) - (i + 1)))):
                if tail_columns[i] == None:
                    tail_columns[i] = lastrecord[i]
                    #print(i)
                else:
                    self.index.updated[i-4] = 1
        # add the tail record and remember its location
        tail_page_number, offset = page_range.add_tail_record(tail_columns)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [page_range_number, tail_page_number, offset]})
        # return the rid to the caller (mostly used for testing right now)
        return rid
    
    def delete_record(self, rid):

        # if the rid is not in the page directory, return false
        if (rid not in self.page_directory.keys()):
            return False
        
        # get the location of the page
        page_range_number, base_page_number, offset = self.page_directory.get(rid)
        
        # change the indirection column to a 0 value
        self.page_ranges[page_range_number].change_indirection(base_page_number, offset, 0)

        # remove the page from the page directory
        self.delete_rid(rid)

        # if the indirection was successfully changed return true
        return True
        
    def get_indirection(self, rid):
        page_range_number, base_page_number, offset = self.page_directory.get(rid)
        return self.page_ranges[page_range_number].read_base_record(base_page_number, offset)[INDIRECTION_COLUMN]
    
    def get_new_rid(self):

        # update and return the rid
        if(len(self.deletedrids) > 0):
            return self.deletedrids.pop()
        self.last_rid += 1
        return self.last_rid

    def delete_rid(self, rid):
        self.baserids.remove(rid)
        self.page_directory.pop(rid)
        self.deletedrids.insert(0, rid)

    def __merge(self):
        #milestone2 we don't care lets goo
        print("merge is happening")
        pass
