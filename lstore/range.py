from lstore.bufferpool import Bufferpool
from lstore.page import Page
from lstore.config import *

class Range:

    def __init__(self, num_columns, primary_key, table_name, range_number):

        # set the number of columns
        self.num_columns = num_columns

        # set the column number of the primary key
        self.primary_key = primary_key

        # record the name of the table to which this range belongs
        self.table_name = table_name

        # record the number in the list of ranges that corresponds to this range
        self.range_number = range_number

        # set the number of records in this range
        self.num_records = 0

        # note the currently filling base page
        self.current_base_page = -1

        # currently filling tail page
        self.current_tail_page = -1

        # generate an initial empty base page
        self.add_base_page()
        self.add_tail_page()

    def add_base_record(self, columns):
        # update each of the columns
        for column_num in range(self.num_columns):

            # get the most recent page for reading
            page = Bufferpool().hold_base_page(self.table_name, self.range_number, column_num, self.current_base_page, False)

            # if the page has no space to be written to
            if page.has_capacity() != True:

                # release the currently held page since it is not the one that we want
                Bufferpool().release_base_page(self.table_name, self.range_number, column_num, self.current_base_page)

                # then add a base page
                self.add_base_page()

                # load the new empty base page
                page = Bufferpool().hold_base_page(self.table_name, self.range_number, column_num, self.current_base_page, True)

            # if the page does have space to be written to
            else:

                # release the base page for reading
                Bufferpool().release_base_page(self.table_name, self.range_number, column_num, self.current_base_page)

                # get the page for writing
                page = Bufferpool().hold_base_page(self.table_name, self.range_number, column_num, self.current_base_page, True)

            # write to it
            offset = page.write(columns[column_num])

            # release the page after it is no longer needed
            Bufferpool().release_base_page(self.table_name, self.range_number, column_num, self.current_base_page)
        
        # increase the number of records
        self.num_records += 1

        # return the offset and page number within the page range
        return self.current_base_page, offset
    
    def add_base_page(self):

        # update the current base page value
        self.current_base_page += 1

        # add a new base page which means one more page in each of the columns
        for column_num in range(self.num_columns):

            # add a new base page
            Bufferpool().add_base_page(self.table_name, self.range_number, column_num, self.current_base_page)
    
    def read_base_record(self, base_page_number, offset):

        # declare a variable to hold the record contents
        columns = []

        # collect the value for each of the columns
        for column_num in range(self.num_columns):

            # read the value from the correct page
            columns.append(Bufferpool().hold_base_page(self.table_name, self.range_number, column_num, base_page_number, False).read(offset))
            Bufferpool().release_base_page(self.table_name, self.range_number, column_num, base_page_number)

        # return the full record
        return columns

    def add_tail_record(self, columns):
        # update each of the columns
        for column_num in range(self.num_columns):

            # get the most recent page for reading
            page = Bufferpool().hold_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page, False)

            # if the page has no space to be written to
            if page.has_capacity() != True:

                # release the currently held page since it is not the one that we want
                Bufferpool().release_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page)

                # then add a tail page
                self.add_tail_page()

                # load the new empty tail page
                page = Bufferpool().hold_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page, True)

            # if the page does have space to be written to
            else:

                # release the base page for reading
                Bufferpool().release_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page)

                # get the page for writing
                page = Bufferpool().hold_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page, True)

            # write to it
            offset = page.write(columns[column_num])

            # release the page after it is no longer needed
            Bufferpool().release_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page)

        # return the offset and page number within the page range
        return self.current_tail_page, offset

    def add_tail_page(self):

        # update the current base page value
        self.current_tail_page += 1

        # add a new base page which means one more page in each of the columns
        for column_num in range(self.num_columns):

            # add a new tail page
            Bufferpool().add_tail_page(self.table_name, self.range_number, column_num, self.current_tail_page)


    def read_tail_record(self, tail_page_number, offset):
        # declare a variable to hold the record contents
        columns = []

        # collect the value for each of the columns
        for column_num in range(self.num_columns):

            # read the value from the correct page
            columns.append(Bufferpool().hold_tail_page(self.table_name, self.range_number, column_num, tail_page_number, False).read(offset))
            Bufferpool().release_tail_page(self.table_name, self.range_number, column_num, tail_page_number)

        # return the full record
        return columns

    def change_indirection(self, base_page_number, offset, value):
        page = Bufferpool().hold_base_page(self.table_name, self.range_number, INDIRECTION_COLUMN, base_page_number, True)
        page.write_at_offset(value, offset)
        Bufferpool().release_base_page(self.table_name, self.range_number, INDIRECTION_COLUMN, base_page_number)
        return True
    
    def change_og_rid(self, base_page_number, offset, value):
        page = Bufferpool().hold_base_page(self.table_name, self.range_number, OG_RID_COLUMN, base_page_number, True)
        page.write_at_offset(value, offset)
        Bufferpool().release_base_page(self.table_name, self.range_number, OG_RID_COLUMN, base_page_number)
        return True

    def change_schema_encoding(self, base_page_number, offset, value):
        page = Bufferpool().hold_base_page(self.table_name, self.range_number, SCHEMA_ENCODING_COLUMN, base_page_number, True)
        page.write_at_offset(value, offset)
        Bufferpool().release_base_page(self.table_name, self.range_number, SCHEMA_ENCODING_COLUMN, base_page_number)
        return True


    def has_capacity(self):

        # returns true if there is capacity, false otherwise
        if self.num_records < 65536:
            return True
        
        else:
            return False
