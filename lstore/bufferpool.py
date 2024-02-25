from lstore.page import Page
import queue
import os
import struct
from lstore.config import *

class Bufferpool:
    _instance = None

    def __new__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super(Bufferpool, self).__new__(self)
            self._instance.__initialized = False
        return self._instance
    
    def __init__(self):
        if not self.__initialized:
            self.pool = queue.Queue(BUFFERPOOL_SIZE)
            self.__initialized = True

    def set_path(self, path):
        self.path = path

    def hold_base_page(self, table, page_range, column_num, page_num):
        pass

    def release_base_page(self, table, page_range, column_num, page_num):
        pass

    def hold_tail_page(self, table, page_range, column_num, page_num):
        pass
    
    def release_tail_page(self, table, page_range, column_num, page_num):
        pass
    
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

    def read_num_base_records(self, table, page_range, page_num):
        path = os.path.join(self.path, table, "PageRange" + str(page_range), "BasePageRecordCounts.bin")

        with open(path, 'rb') as file:
            file.read(page_num * 2)
            data = file.read(2)
        
        return struct.unpack('H', data)
            

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
        
        return struct.unpack('H', data)
            

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