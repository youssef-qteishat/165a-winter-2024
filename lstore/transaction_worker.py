from lstore.table import Table, Record
from lstore.index import Index

import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = []
        if transactions != None:
            for i in transactions:
                self.transactions.append(i)
        self.result = 0
        self.thread = threading.Thread(target=self.__run)

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        all_tids = []
        for transaction in self.transactions:
            all_tids.append(transaction.tid)
        self.thread.start()
    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread.is_alive() == False:
            return
        else:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            success, lock_failure = (False, True)
            while lock_failure == True:
                success, lock_failure = transaction.run()
            self.stats.append(success)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

