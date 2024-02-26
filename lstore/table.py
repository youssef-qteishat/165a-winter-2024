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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.baserids = []
        self.page_directory = {}
        self.page_ranges = []
        self.page_ranges.append(Range(6 + self.num_columns, self.key, self.name, len(self.page_ranges)))
        self.last_rid = 0
        self.index = Index(self)
        self.deletedrids = []
        self.update_count = 0

    def insert_record(self, columns):
        """
        Add a new base record to the table with columnar values

        :param columns: []      #List containing column values for record
        """

        # if the page range does not have capacity create a new one
        if self.page_ranges[-1].has_capacity() != True:
            self.page_ranges.append(Range(6 + self.num_columns, self.key, self.name, len(self.page_ranges)))

        # get the latest range
        page_range = self.page_ranges[-1]

        # get a new rid
        rid = self.get_new_rid()

        # load the colomn daat into a list
        tail_columns = [rid, rid, int(1000000 * time()), 0, rid, rid] + columns

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
        #print("read base", base_record)
        # get the indirection column and schema encoding
        tail_record_id = base_record[INDIRECTION_COLUMN]
        #print("tid = ", tail_record_id)
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
            for i in range(6, len(tail_columns)):
                if (not (1 & (schema_encoding >> (len(tail_columns) - (i + 1))))):
                    tail_columns[i] = base_record[i]
            tail_record_id = tail_columns[INDIRECTION_COLUMN]

        return tail_columns

    def update_record(self, columns, baserid):
        
        # if the page range does not have capacity create a new one
        if self.page_ranges[-1].has_capacity() != True:
            self.page_ranges.append(Range(6 + self.num_columns, self.key))

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

        #if first update add copy of base record so that when merged, can still access first version
        if lastrid == baserid:
            copy_rid = self.get_new_rid()
            copy_tail_columns = [baserid, copy_rid, int(1000000* time()), 0, baserid, baserid]+[None]*len(columns)
            copy_tail_page_number, copy_offset = page_range.add_tail_record(copy_tail_columns)
            copy_page_range_number = len(self.page_ranges) - 1
            self.page_directory.update({copy_rid: [copy_page_range_number, copy_tail_page_number, copy_offset]})

        # get a new rid
        rid = self.get_new_rid()
        # update the schema encoding and indirection of the base page
        basepagerange, basepagenum, offset = self.page_directory[baserid]
        self.page_ranges[basepagerange].change_indirection(basepagenum, offset, rid)
        self.page_ranges[basepagerange].change_schema_encoding(basepagenum, offset, schema_encoding)

        # load the colomn daat into a list
        tail_columns = [lastrid, rid, int(1000000 * time()), schema_encoding, baserid, baserid] + columns
        for i in range(6, len(tail_columns)):
            if (1 & (schema_encoding >> (len(tail_columns) - (i + 1)))):
                if tail_columns[i] == None:
                    tail_columns[i] = lastrecord[i]
                else:
                    self.index.updated[i-6] = 1
        #print(tail_columns)
        # add the tail record and remember its location
        tail_page_number, offset = page_range.add_tail_record(tail_columns)
        page_range_number = len(self.page_ranges) - 1

        # store the rid and location of the record in the page directory
        self.page_directory.update({rid: [page_range_number, tail_page_number, offset]})
        # return the rid to the caller (mostly used for testing right now)
        self.update_count+=1
        #print(self.update_count)
        if(self.update_count > 1000):
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
        #print("merge is happening")
        mergedrids = []
        page_range_count = len(self.page_ranges)
        for page_range_num in reversed(range(page_range_count)):
            for tail_page_num in range(self.page_ranges[page_range_num].current_tail_page+1):
                tail_page = Bufferpool().hold_tail_page(self.name, page_range_num, 0, tail_page_num)
                offset = tail_page.num_records*8
                Bufferpool().release_tail_page(self.name, page_range_num, 0, tail_page_num)
                while offset > 0:
                    offset -= 8
                    tail_record = self.page_ranges[page_range_num].read_tail_record(tail_page_num, offset)
                    base_rid = tail_record[BASERID_COLUMN]
                    #only merge if not already visited
                    if base_rid not in mergedrids:
                        if base_rid not in self.page_directory:
                            continue
                        base_page_range, base_page_num, base_offset = self.page_directory[base_rid]
                        mergedrids.append(base_rid)
                        #only merge full base pages according to piazza https://piazza.com/class/lr5k6jd9o5k5vs/post/47
                        #doesn't matter because we're not merging full pages, just records
                        #if self.page_ranges[base_page_range].base_pages[0][base_page_num].has_capacity:
                            #pass
                        #    continue

                        base_record = self.page_ranges[base_page_range].read_base_record(base_page_num, base_offset)
                        new_columns = base_record[:6]
                        schema_encoding = tail_record[SCHEMA_ENCODING_COLUMN]
                        #print("baserecord=", base_record)
                        for i in range(self.num_columns):
                            if (1 & (schema_encoding >> (self.num_columns - (i + 1)))):
                                new_columns.append(tail_record[i+6])
                            else:
                                new_columns.append(base_record[i+6])
                        #print("new cols", new_columns)
                        if self.page_ranges[-1].has_capacity() != True:
                            self.page_ranges.append(Range(6 + self.num_columns, self.key, self.name, len(self.page_ranges)))
                        page_range = self.page_ranges[-1]

                        new_page_number, new_offset = page_range.add_base_record(new_columns)
                        page_range_number = len(self.page_ranges) - 1
                        #print("new page_range_num = ", page_range_number)
                        # store the rid and location of the record in the page directory
                        self.page_directory.update({base_rid: [page_range_number, new_page_number, new_offset]})
                        #print("merged", base_rid)
                        #print(self.read_record(base_rid))
    
    def __merge_with_tps(self):
        #hashmap or dict for storing RIDs that have already been merged
        mergerids = {}

        # Step 1: select a set of consecutive fully committed tail records since the last merge within the update range
        # ex: merge when # of updates is greater than TPS by 5


        # Step 2: load the cooresponding base pages (that have the base record)
        # For selected set of committed tail records:
        #   load corresponding outdated base pages for given update range
        #   

        # Step 3: Consolidate base and tail pages
        # For every updated column:
        #   the merge process will read n outdated base pages
        #   applies a set of recent committed updates from the tail pages
        #   writes out m new pages.6 
        # First committed tail pages' Base_rid columns (from Step 1) are scanned in reverse order to find the list of the latest version of every updated record since the last merge 
        # (a temporary hashtable may be used to keep track whether the latest version of a record is seen or not)
        # Subsequently, applying the latest tail records in a reverse order to the base records until an update to every record in the base range is seen or the list is exhausted, skipping any intermediate versions for which a newer update exists in the selected tail records. 
        # If a latest tail record indicates the deletion of the record:
        #   the deleted record will be included in the consolidated records

        # Step 4: update the page direcotry
        self.page_directory.update({base_rid: [page_range_number, new_page_number, new_offset]})
        # Step 5: de-allocate the outdated base pages
        pass


    #test function to allow me to call merge in test functions
    def mergetest(self):
        self.__merge()