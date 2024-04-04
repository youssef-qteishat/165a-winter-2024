from lstore.page import Page
import os
import struct
import time
import threading
from lstore.config import *

'''
The bufferpool is represented as a list of 7 element lists.
The pool can contain 16 pages (as defined by config.py) and for
each page stores the file-path of that page (unique to each page),
whether or not it is currently pinned, the page itself, the table name,
the page range, if it is a base or tail page, and the number within the range.

The spec_offset is the speculative offset after all currently aquired locks are 
released, this allows the lock takers to take locks on pages that are not yet created.

[
    [file_path_0, pinned_val, Page(), db_name, table_name, page_range, base_page_bool, page_number, dirty_bool],
    [file_path_1, pinned_val, Page(), db_name, table_name, page_range, base_page_bool, page_number, dirty_bool],
    [file_path_2, pinned_val, Page(), db_name, table_name, page_range, base_page_bool, page_number, dirty_bool],
    ...
    [file_path_15, pinned_val, Page(), db_name, table_name, page_range, base_page_bool, page_number, dirty_bool]
}

calling acquire and release lock when holding and releasing pages is temporary until
we write new functions to aquire all locks for an transaction and then release them all at the end.
this is done to satisfy 2PL protocol
'''

class Index_Lock:
    exclusive = False
    tid = None

class Lock:
    exclusive = False
    offset = None
    tid = None

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
            self.lock = False
            self.lock_lock = threading.Lock()
            self.__initialized = True

    def get_priority(self):
        while(True):
            with self.lock_lock:
                if not self.lock:
                    self.lock = True
                    return
            time.sleep(0.1)
    
    def release_priority(self):
        with self.lock_lock:
            if( not self.lock):
                raise Exception("Faulty Lock")
            self.lock = False
        
    def get_locks(self, page_key):
        self.get_priority()
        if page_key not in self.locks.keys():
            self.release_priority()
            return []
        else:
            self.release_priority()
            return self.locks[page_key]

    def acquire_lock(self, tid, page_key, offset, exclusive_bool):
        self.get_priority()
        if page_key not in self.locks.keys():
            self.locks[page_key] =  []
        else:
            for lock in self.locks[page_key]:
                if ((lock.exclusive and exclusive_bool) and (lock.tid != tid)):
                    self.release_priority()
                    return False
                if ((lock.offset == offset) and (lock.exclusive or exclusive_bool) and (lock.tid != tid)):
                    self.release_priority()
                    return False
        lock = Lock()
        lock.exclusive = exclusive_bool
        lock.offset = offset
        lock.tid = tid
        self.locks[page_key].append(lock)
        self.release_priority()
        return True
            #self.locks[page_num]:  # Wait until lock is released (lock status becomes False)

    def release_lock(self, tid, page_key, offset, exclusive_bool):
        self.get_priority()
        if page_key in self.locks.keys():
            for lock in self.locks[page_key]:
                if (lock.tid == tid and lock.offset == offset and lock.exclusive == exclusive_bool):
                    db_name, table_name, page_range, base_page_bool, page_number = page_key
                    for page in self.pool:
                        if db_name == page[3] and table_name == page[4] and page_range == page[5] and base_page_bool == page[6] and page_number == page[7]:
                            if page[1] == 0:
                                if page[8] == True:
                                    with open(page[0], 'wb') as file:
                                        file.write(page[2].data)
                                    if (page[6] == True):
                                        self.write_num_base_records(page[3], page[4], page[5], page[7], page[2].num_records)
                                    else:
                                        self.write_num_tail_records(page[3], page[4], page[5], page[7], page[2].num_records)
                                    page[8] = False
                    self.locks[page_key].remove(lock)
                    if (not self.locks[page_key]):
                        self.locks.pop(page_key)
                    self.release_priority()
                    return True
        self.release_priority()
        return False


    def hold_base_page(self, db, table, page_range, column_num, page_num, dirty_bool, page_only):
        self.get_priority()
        file_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num), "BasePage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            if len(self.pool) >= BUFFERPOOL_SIZE:
                self.evict_page()
            with open(file_path, 'rb') as file:
                data = bytearray(file.read())
            num_records = self.read_num_base_records(db, table, page_range, page_num)
            # upon being loaded the page is pinned by a single page range
            page = [file_path, 1, Page(num_records, data), db, table, page_range, True, page_num, dirty_bool]
            self.pool.insert(0, page)
        
        else:
            self.pool.remove(page)
            self.pool.insert(0, page)
            page[1] += 1
            page[8] = dirty_bool | page[8]

        self.release_priority()
        if page_only:
            return page[2]
        else:
            return page

    def release_base_page(self, db, table, page_range, column_num, page_num):
        self.get_priority()
        file_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num), "BasePage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)
        
        if page == None:
            raise Exception
        
        else:
            page[1] -= 1
        self.release_priority()
    

    def hold_tail_page(self, db, table, page_range, column_num, page_num, dirty_bool, page_only):
        self.get_priority()
        file_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num), "TailPage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            if len(self.pool) >= BUFFERPOOL_SIZE:
                self.evict_page()
            with open(file_path, 'rb') as file:
                data = bytearray(file.read())
            num_records = self.read_num_tail_records(db, table, page_range, page_num)
            # upon being loaded the page is pinned by a single page range
            page = [file_path, 1, Page(num_records, data), db, table, page_range, False, page_num, dirty_bool]
            self.pool.insert(0, page)
        
        else:
            self.pool.remove(page)
            self.pool.insert(0, page)
            page[1] += 1
            page[8] = dirty_bool | page[8]

        self.release_priority()
        if page_only:
            return page[2]
        else:
            return page

    def release_tail_page(self, db, table, page_range, column_num, page_num):
        self.get_priority()
        file_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num), "TailPage" + str(page_num) + ".bin")
        page = next((page for page in self.pool if page[0] == file_path), None)

        if page == None:
            raise Exception
        
        else:
            page[1] -= 1
        self.release_priority()
    
    def add_base_page(self, db, table, page_range, column_num, page_num):
        self.get_priority()
        directory_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num))
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, "BasePage" + str(page_num) + ".bin")
        self.write_num_base_records(db, table, page_range, page_num, 0)
        with open(file_path, 'wb') as file:
            file.write(bytearray(PAGESIZE))
        self.release_priority()

    def add_tail_page(self, db, table, page_range, column_num, page_num):
        self.get_priority()
        directory_path = os.path.join(db, table, "PageRange" + str(page_range), "Column" + str(column_num))
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, "TailPage" + str(page_num) + ".bin")
        self.write_num_tail_records(db, table, page_range, page_num, 0)
        with open(file_path, 'wb') as file:
            file.write(bytearray(PAGESIZE))
        self.release_priority()

    def dump_pool(self):
        self.get_priority()
        while len(self.pool) != 0:
            self.evict_page()

        if len(self.pool) != 0:
            raise Exception("Cannot dump bufferpool")
        self.release_priority()

    def read_num_base_records(self, db, table, page_range, page_num):
        path = os.path.join(db, table, "PageRange" + str(page_range), "BasePageRecordCounts.bin")

        with open(path, 'rb') as file:
            file.read(page_num * 2)
            data = file.read(2)
        
        return struct.unpack('H', data)[0]
            

    def write_num_base_records(self, db, table, page_range, page_num, value):

        path = os.path.join(db, table, "PageRange" + str(page_range), "BasePageRecordCounts.bin")

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

    def read_num_tail_records(self, db, table, page_range, page_num):
        path = os.path.join(db, table, "PageRange" + str(page_range), "TailPageRecordCounts.bin")

        with open(path, 'rb') as file:
            file.read(page_num * 2)
            data = file.read(2)
        
        return struct.unpack('H', data)[0]
            

    def write_num_tail_records(self, db, table, page_range, page_num, value):

        path = os.path.join(db, table, "PageRange" + str(page_range), "TailPageRecordCounts.bin")

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
            exclusive_lock = False
            if page[0] in self.locks.keys():
                for lock in self.locks[page[0]]:
                    if (lock.exclusive):
                        exclusive_lock = True
            if (exclusive_lock):
                continue
            if page[1] == 0:
                if page[8] == True:
                    with open(page[0], 'wb') as file:
                        file.write(page[2].data)
                    if (page[6] == True):
                        self.write_num_base_records(page[3], page[4], page[5], page[7], page[2].num_records)
                    else:
                        self.write_num_tail_records(page[3], page[4], page[5], page[7], page[2].num_records)
                    self.pool.remove(page)
                else:
                    self.pool.remove(page)
                return True
        raise Exception("Deadlock has occurred")
        

    
    
