from lstore.page import Page
from lstore.config import *

class Range:

    def __init__(self, num_columns, primary_key):

        # set the number of columns
        self.num_columns = num_columns

        # set the column number of the primary key
        self.primary_key = primary_key

        # set the number of records in this range
        self.num_records = 0

        # create a place to hold the pages
        self.base_pages = {}
        self.tail_pages = {}
        # add the first empty base page
        for column_num in range(num_columns):
            self.base_pages[column_num] = [Page()]
        for column_num in range(num_columns):
            self.tail_pages[column_num] = [Page()]
        # note the currently filling base page
        self.current_base_page = 0
        # currently filling tail page
        self.current_tail_page = 0

    def add_base_record(self, columns):
        # update each of the columns
        for column_num in range(self.num_columns):

            # get the most recent page
            page = self.base_pages[column_num][self.current_base_page]

            # if the page has no space to be written to
            if page.has_capacity() != True:

                # then add a base page
                self.add_base_page()
                page = self.base_pages[column_num][self.current_base_page]

            # write to it
            offset = page.write(columns[column_num])

            # increase the number of records
            self.num_records += 1

        # return the offset and page number within the page range
        return self.current_base_page, offset
    
    def read_base_record(self, base_page_number, offset):

        # declare a variable to hold the record contents
        columns = []

        # collect the value for each of the columns
        for column_num in range(self.num_columns):

            # read the value from the correct page
            columns.append(self.base_pages[column_num][base_page_number].read(offset))

        # return the full record
        return columns
   

    def add_base_page(self):

        # add a new base page which means one more page in each of the columns
        for column_num in range(self.num_columns):

            # add a new base page
            self.base_pages[column_num].append(Page())

        # update the current base page value
        self.current_base_page += 1

    def add_tail_record(self, columns):
        # update each of the columns
        for column_num in range(self.num_columns):

            # get the most recent page
            page = self.tail_pages[column_num][self.current_tail_page]

            # if the page has no space to be written to
            if page.has_capacity() != True:

                # then add a base page
                self.add_tail_page()
                page = self.tail_pages[column_num][self.current_tail_page]

            # write to it
            offset = page.write(columns[column_num])
            self.num_records += 1
        # return the offset and page number within the page range
        return self.current_tail_page, offset

    def add_tail_page(self):
        # add a new base page which means one more page in each of the columns
        for column_num in range(self.num_columns):

            # add a new base page
            self.tail_pages[column_num].append(Page())

        # update the current base page value
        self.current_tail_page += 1

    #prolly make config file to store all the values like indirectioncolumn later
    def change_indirection(self, base_page_number, offset, value):
        self.base_pages[0][base_page_number].write_at_offset(value, offset)
        return True
    
    def change_schema_encoding(self, base_page_number, offset, value):
        self.base_pages[3][base_page_number].write_at_offset(value, offset)
        return True

    def read_tail_record(self, tail_page_number, offset):
        # declare a variable to hold the record contents
        columns = []

        # collect the value for each of the columns
        for column_num in range(self.num_columns):

            # read the value from the correct page
            columns.append(self.tail_pages[column_num][tail_page_number].read(offset))

        # return the full record
        return columns


    def has_capacity(self):

        # returns true if there is capacity, false otherwise
        if self.num_records < 65536:
            return True
        
        else:
            return False
