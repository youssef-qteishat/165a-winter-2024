from lstore.config import *

class Page:
    
    def __init__(self, num_records, data):
        self.num_records = num_records
        self.data = data

    def has_capacity(self):
        return self.num_records < PAGECAP

    def write(self, value):
        #all columns are 64 bit ints so 8 bytes
        if value == None:
            value = 0
        offset = self.num_records*8
        self.data[offset:offset+8] = value.to_bytes(8, 'big')
        self.num_records += 1
        return offset
    #for changing the indirection column
    def write_at_offset(self, value, offset):
        self.data[offset:offset+8] = value.to_bytes(8, 'big')
        return offset
    def read(self, offset):
        return int.from_bytes(self.data[offset:offset + 8], 'big')
