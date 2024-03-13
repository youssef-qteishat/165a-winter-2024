from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.bufferpool import Bufferpool

import time

counter = 0

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        global counter
        self.queries = []
        self.locks = []
        self.tid = counter
        counter += 1

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        flag = False
        for i in range(0, 10): 
            flag = self.aquire()
            if flag == True:
                break
            self.release()
            time.sleep(0.1)
        if flag == False:
            return self.abort()
            
        for query, table, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()

        return self.commit()

    
    def abort(self):
        self.release()
        return False

    
    def commit(self):
        self.release()
        return True
    
    def aquire(self):
        for query, table, args in self.queries:
            success = None
            operation_locks = None
            if query.__func__ is Query.delete:
                pass
            if query.__func__ is Query.insert:
                success, operation_locks = Query.aquire_insert_locks(table, self.tid, args)
            if query.__func__ is Query.select:
                pass
            if query.__func__ is Query.select_version:
                pass
            if query.__func__ is Query.update:
                pass
            if query.__func__ is Query.sum:
                pass
            if query.__func__ is Query.select_version:
                pass
            if query.__func__ is Query.increment:
                pass
            self.locks.extend(operation_locks)
            if success is False:
                return False
            return True
    
    def release(self):
        for lock in self.locks:
            Bufferpool().release_lock(lock[0], lock[1], lock[2], lock[3])