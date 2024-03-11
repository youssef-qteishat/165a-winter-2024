from lstore.index import Index
from time import time
from lstore.range import Range
from lstore.bufferpool import Bufferpool
from lstore.config import *

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    
    def __repr__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, db_path):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.db_path = db_path
        self.baserids = []
        self.page_directory = {}
        self.page_ranges = []
        self.page_ranges.append(Range(7 + self.num_columns, self.key, self.db_path, self.name, len(self.page_ranges)))
        self.last_rid = 0
        self.index = Index(self)
        self.update_count = 0

    def insert_record(self, columns):
        """
        Add a new base record to the table with columnar values

        :param columns: []      #List containing column values for record
        """

        # if the page range does not have capacity create a new one
        if self.page_ranges[-1].has_capacity() != True:
            self.page_ranges.append(Range(7 + self.num_columns, self.key, self.db_path, self.name, len(self.page_ranges)))

        # get the latest range
        page_range = self.page_ranges[-1]

        # get a new rid
        rid = self.get_new_rid()
        tail_columns = [rid, rid, int(1000000 * time()), 0, rid, rid, rid] + columns

        # add the base record and remember its location
        base_page_number, offset = page_range.add_base_record(tail_columns)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [page_range_number, base_page_number, offset]})
        self.baserids.append(rid)
        #add to index
        for i in range(len(columns)):
            self.index.addToIndex(i, columns[i], rid)
            
        if self.page_ranges[-1].has_capacity() != True:
            self.page_ranges.append(Range(7 + self.num_columns, self.key, self.db_path, self.name, len(self.page_ranges)))


        # return the rid to the caller (mostly used for testing right now)
        return rid

    def read_record(self, rid, version):
        # get the location of the base page by its rid
        page_range_number, base_page_number, offset = self.page_directory.get(rid)
        base_record = self.page_ranges[page_range_number].read_base_record(base_page_number, offset)
        tps = base_record[TPS_COLUMN]
        schema_encoding = base_record[SCHEMA_ENCODING_COLUMN]
        
        #making copy here to make it easier to get later
        if base_record[OG_RID_COLUMN] != base_record[RID_COLUMN]:
            og_page_range_number, og_base_page_number, og_offset = self.page_directory.get(base_record[OG_RID_COLUMN])
            og_columns = self.page_ranges[og_page_range_number].read_tail_record(og_base_page_number, og_offset)
        else:
            og_columns = base_record
        # get the indirection column and schema encoding
        tail_record_id = base_record[INDIRECTION_COLUMN]
        if tail_record_id <= tps and version == 0:
            return base_record
        for i in range(version-1, 0):
            if tail_record_id == rid:
            # read the record from the location
                return og_columns
            # get the location from the page directory
            tail_page_range_number, tail_page_number, tail_offset = self.page_directory.get(tail_record_id)
            # get the tail record columns
            tail_columns = self.page_ranges[tail_page_range_number].read_tail_record(tail_page_number, tail_offset)
            #use the schema encoding to reconstruct full record
            schema_encoding = tail_columns[SCHEMA_ENCODING_COLUMN]
            for i in range(7, len(tail_columns)):
                if (not (1 & (schema_encoding >> (len(tail_columns) - (i + 1))))):
                    tail_columns[i] = og_columns[i]
            tail_record_id = tail_columns[INDIRECTION_COLUMN]
        
        return tail_columns

    def update_record(self, columns, baserid):
        
        lastrecord = self.read_record(baserid, 0)
        lastrid = lastrecord[RID_COLUMN]
        ogrid = lastrecord[OG_RID_COLUMN]

        # get the schema encoding
        binary_string = ''
        for value in columns:
            if value is not None:
                binary_string += '1'
            else:
                binary_string += '0'
        schema_encoding = int(binary_string, 2) | lastrecord[SCHEMA_ENCODING_COLUMN]
        

        # update the schema encoding and indirection of the base page
        basepagerange, basepagenum, baseoffset = self.page_directory[baserid]
        base_record = self.page_ranges[basepagerange].read_base_record(basepagenum, baseoffset)
        if lastrid == baserid:
            copy_rid = self.get_new_rid()
            copy_tail_columns = [baserid, copy_rid, int(1000000* time()), 0, baserid, baserid, baserid]+base_record[7:]
            copy_tail_page_number, copy_offset = self.page_ranges[basepagerange].add_tail_record(copy_tail_columns)
            self.page_directory.update({copy_rid: [basepagerange, copy_tail_page_number, copy_offset]})
            self.page_ranges[basepagerange].change_og_rid(basepagenum, baseoffset, copy_rid)
        
        
        rid = self.get_new_rid()
        self.page_ranges[basepagerange].change_indirection(basepagenum, baseoffset, rid)
        self.page_ranges[basepagerange].change_schema_encoding(basepagenum, baseoffset, schema_encoding)

        # load the colomn daat into a list
        tail_columns = [lastrid, rid, int(1000000 * time()), schema_encoding, baserid, baserid, ogrid] + columns
        for i in range(7, len(tail_columns)):
            if (1 & (schema_encoding >> (len(tail_columns) - (i + 1)))):
                if tail_columns[i] == None:
                    tail_columns[i] = lastrecord[i]
                else:
                    self.index.update_index(i-7, tail_columns[i], lastrecord[i], baserid)
        # add the tail record and remember its location
        tail_page_number, offset = self.page_ranges[basepagerange].add_tail_record(tail_columns)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [basepagerange, tail_page_number, offset]})
        # return the rid to the caller (mostly used for testing right now)
        self.update_count+=1
        if(self.update_count > MERGE_INTERVAL):
            self.__merge()
            self.update_count = 0
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
        for column_number in range(self.num_columns):
            self.index.updated[column_number] = 1

        # if the indirection was successfully changed return true
        return True
        
    def get_indirection(self, rid):
        page_range_number, base_page_number, offset = self.page_directory.get(rid)
        return self.page_ranges[page_range_number].read_base_record(base_page_number, offset)[INDIRECTION_COLUMN]
    
    def get_new_rid(self):

        # update and return the rid
        self.last_rid += 1
        return self.last_rid

    def delete_rid(self, rid):
        self.baserids.remove(rid)
        self.page_directory.pop(rid)

    def __merge(self):
        print("merge is happening")
        mergedrids = []
        page_range_count = len(self.page_ranges)
        for page_range_num in reversed(range(page_range_count)):
            for tail_page_num in range(self.page_ranges[page_range_num].current_tail_page+1):
                tail_page = Bufferpool().hold_tail_page(self.db_path, self.name, page_range_num, 0, tail_page_num, False)
                offset = tail_page.num_records*8
                Bufferpool().release_tail_page(self.db_path, self.name, page_range_num, 0, tail_page_num)
                while offset > 0:
                    offset -= 8
                    tail_record = self.page_ranges[page_range_num].read_tail_record(tail_page_num, offset)
                    base_rid = tail_record[BASERID_COLUMN]
                    tail_rid = tail_record[RID_COLUMN]
                    #only merge if not already visited
                    if base_rid not in mergedrids:
                        if base_rid not in self.page_directory:
                            continue
                        base_page_range, base_page_num, base_offset = self.page_directory[base_rid]
                        mergedrids.append(base_rid)

                        base_record = self.page_ranges[base_page_range].read_base_record(base_page_num, base_offset)
                        if tail_rid <= base_record[TPS_COLUMN]:
                            print("merge less")
                            return
                        og_page_range, og_page_num, og_offset = self.page_directory[base_record[OG_RID_COLUMN]]
                        og_record = self.page_ranges[og_page_range].read_tail_record(og_page_num, og_offset)
                        new_columns = base_record[:5]
                        #appends base rid of tail record to new columns (i.e fields) of updated record, becoming merged reecord's tps
                        new_columns.append(tail_rid)
                        new_columns.append(base_record[OG_RID_COLUMN])
                        schema_encoding = tail_record[SCHEMA_ENCODING_COLUMN]
                        for i in range(self.num_columns):
                            if (1 & (schema_encoding >> (self.num_columns - (i + 1)))):
                                new_columns.append(tail_record[i+7])
                            else:
                                new_columns.append(og_record[i+7])
                        #print(new_columns)
                        if self.page_ranges[-1].has_capacity() != True:
                            self.page_ranges.append(Range(7 + self.num_columns, self.key, self.db_path, self.name, len(self.page_ranges)))
                        page_range = self.page_ranges[-1]
                        new_page_number, new_offset = page_range.add_base_record(new_columns)
                        page_range_number = len(self.page_ranges) - 1
                        # store the rid and location of the record in the page directory
                        self.page_directory.update({base_rid: [page_range_number, new_page_number, new_offset]})
        print("merged!")


    #test function to allow me to call merge in test functions
    def mergetest(self):
        self.__merge()
