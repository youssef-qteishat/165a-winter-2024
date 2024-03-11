from lstore.page import Page
import os
import struct
from lstore.config import *

'''
The bufferpool is represented as a list of 7 element lists.
The pool can contain 16 pages (as defined by config.py) and for
each page stores the file-path of that page (unique to each page),
whether or not it is currently pinned, the page itself, the table name,
the page range, if it is a base or tail page, and the number within the range.

[
    [file_path_0, pinned_val, Page(), table_name, page_range, base_page_bool, page_number, dirty_bool],
    [file_path_1, pinned_val, Page(), table_name, page_range, base_page_bool, page_number, dirty_bool],
    [file_path_2, pinned_val, Page(), table_name, page_range, base_page_bool, page_number, dirty_bool],
    ...
    [file_path_15, pinned_val, Page(), table_name, page_range, base_page_bool, page_number, dirty_bool]
}

calling acquire and release lock when holding and releasing pages is temporary until
we write new functions to aquire all locks for an transaction and then release them all at the end.
this is done to satisfy 2PL protocol
'''
class Bufferpool:
    _instance = None

    def __new__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super(Bufferpool, self).__new__(self)
            self._instance.__initialized = False
        return self._instance
    
    def __init__(self):
        if not self.__initialized:
            self.pool = []
            self.locks = {} # stores locks held on pages on disk
            self.__initialized = True

    def set_path(self, path):
        self.path = path

    def acquire_lock(self, page_num):
        if page_num not in self.locks:
            self.locks[page_num] = False  # Initialize lock status to False (unlocked)
        while self.locks[page_num]:  # Wait until lock is released (lock status becomes False)
            pass
        self.locks[page_num] = True  # Set lock status to True (locked)

    def release_lock(self, page_num):
        if self.locks[page_num]:  # Check if the page is locked (i.e., the lock status is True)
            self.locks[page_num] = False  # Set lock status to False (unlocked)
            
            # Find the page in the pool
            for page in self.pool:
                if page[6] == page_num:  # Check if the page number matches
                    if page[7]:  # Check if the page is dirty
                        # Write the page to disk
                        with open(page[0], 'wb') as file:
                            file.write(page[2].data)
                        # Reset the dirty flag
                        page[7] = False
                    break


    def hold_base_page(self, table, page_range, column_num, page_num, dirty_bool):
        self.acquire_lock(page_num)
        file_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num), "BasePage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            if len(self.pool) >= BUFFERPOOL_SIZE:
                self.evict_page()
            with open(file_path, 'rb') as file:
                data = bytearray(file.read())
            num_records = self.read_num_base_records(table, page_range, page_num)
            # upon being loaded the page is pinned by a single page range
            page = [file_path, 1, Page(num_records, data), table, page_range, True, page_num, dirty_bool]
            self.pool.insert(0, page)
            return page[2]
        
        else:
            self.pool.remove(page)
            self.pool.insert(0, page)
            page[1] += 1
            page[7] = dirty_bool | page[7]
            return page[2]

    def release_base_page(self, table, page_range, column_num, page_num):
        self.release_lock(page_num)
        file_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num), "BasePage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)
        
        if page == None:
            raise Exception
        
        else:
            page[1] -= 1
    

    def hold_tail_page(self, table, page_range, column_num, page_num, dirty_bool):
        self.acquire_lock(page_num)
        file_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num), "TailPage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            if len(self.pool) >= BUFFERPOOL_SIZE:
                self.evict_page()
            with open(file_path, 'rb') as file:
                data = bytearray(file.read())
            num_records = self.read_num_tail_records(table, page_range, page_num)
            # upon being loaded the page is pinned by a single page range
            page = [file_path, 1, Page(num_records, data), table, page_range, False, page_num, dirty_bool]
            self.pool.insert(0, page)
            return page[2]
        
        else:
            self.pool.remove(page)
            self.pool.insert(0, page)
            page[1] += 1
            page[7] = dirty_bool | page[7]
            return page[2]

    def release_tail_page(self, table, page_range, column_num, page_num):
        self.release_lock(page_num)
        file_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num), "TailPage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            raise Exception
        
        else:
            page[1] -= 1
    
    def add_base_page(self, table, page_range, column_num, page_num):
        directory_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num))
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, "BasePage" + str(page_num) + ".bin")
        self.write_num_base_records(table, page_range, page_num, 0)
        with open(file_path, 'wb') as file:
            file.write(bytearray(PAGESIZE))

    def add_tail_page(self, table, page_range, column_num, page_num):
        directory_path = os.path.join(self.path, table, "PageRange" + str(page_range), "Column" + str(column_num))
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, "TailPage" + str(page_num) + ".bin")
        self.write_num_tail_records(table, page_range, page_num, 0)
        with open(file_path, 'wb') as file:
            file.write(bytearray(PAGESIZE))

    def dump_pool(self):

        while len(self.pool) != 0:
            self.evict_page()

        if len(self.pool) != 0:
            raise Exception("Cannot dump bufferpool")

    def read_num_base_records(self, table, page_range, page_num):
        path = os.path.join(self.path, table, "PageRange" + str(page_range), "BasePageRecordCounts.bin")

        with open(path, 'rb') as file:
            file.read(page_num * 2)
            data = file.read(2)
        
        return struct.unpack('H', data)[0]
            

    def write_num_base_records(self, table, page_range, page_num, value):

        path = os.path.join(self.path, table, "PageRange" + str(page_range), "BasePageRecordCounts.bin")

        if os.path.exists(path):
            with open(path, 'rb') as file:
                data_before = file.read(page_num * 2)
                file.read(2)
                data_after = file.read()

            modified_data = data_before + struct.pack('H', value) + data_after

        else:
            # in the case the file does not exist yet write to the first line (we assume this is the first page in the range)
            modified_data = struct.pack('H', value)

        with open(path, 'wb') as file:
            file.write(modified_data)

    def read_num_tail_records(self, table, page_range, page_num):
        path = os.path.join(self.path, table, "PageRange" + str(page_range), "TailPageRecordCounts.bin")

        with open(path, 'rb') as file:
            file.read(page_num * 2)
            data = file.read(2)
        
        return struct.unpack('H', data)[0]
            

    def write_num_tail_records(self, table, page_range, page_num, value):

        path = os.path.join(self.path, table, "PageRange" + str(page_range), "TailPageRecordCounts.bin")

        if os.path.exists(path):
            with open(path, 'rb') as file:
                data_before = file.read(page_num * 2)
                file.read(2)
                data_after = file.read()

            modified_data = data_before + struct.pack('H', value) + data_after

        else:
            # in the case the file does not exist yet write to the first line (we assume this is the first page in the range)
            modified_data = struct.pack('H', value)

        with open(path, 'wb') as file:
            file.write(modified_data)

    def evict_page(self):
        
        for page in reversed(self.pool):
            if page[1] == 0:
                if page[7] == True:
                    with open(page[0], 'wb') as file:
                        file.write(page[2].data)
                    if (page[5] == True):
                        self.write_num_base_records(page[3], page[4], page[6], page[2].num_records)
                    else:
                        self.write_num_tail_records(page[3], page[4], page[6], page[2].num_records)
                    self.pool.remove(page)
                else:
                    self.pool.remove(page)
                return True
        return False

    
    
