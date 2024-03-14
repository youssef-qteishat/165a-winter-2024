from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed
from lstore.transaction import Transaction

import threading

db = Database()

db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

transaction = []
transaction.append(Transaction())
transaction.append(Transaction())
transaction[0].add_query(query.insert, grades_table, *[1, 2, 3, 4, 5])
transaction[0].add_query(query.insert, grades_table, *[6, 5, 8, 9, 10])
transaction[1].add_query(query.insert, grades_table, *[11, 12, 13, 14, 15])
transaction[1].add_query(query.insert, grades_table, *[16, 17, 18, 19, 20])

def run_trans(transaction):
    transaction.run()

# Create threads for running transactions
thread1 = threading.Thread(target=run_trans, args=(transaction[0],))
thread2 = threading.Thread(target=run_trans, args=(transaction[1],))

# Start both threads
thread1.start()
thread2.start()

# Wait for both threads to finish
thread1.join()
thread2.join()


transaction2 = []
transaction2.append(Transaction())
transaction2.append(Transaction())
transaction2[0].add_query(query.select, grades_table, 1, 0, [1, 1, 1, 1, 1])
transaction2[0].add_query(query.select, grades_table, 6, 0, [1, 1, 1, 1, 1])
transaction2[1].add_query(query.select, grades_table, 11, 0, [1, 1, 1, 1, 1])
transaction2[1].add_query(query.select, grades_table, 16, 0, [1, 1, 1, 1, 1])

thread3 = threading.Thread(target=run_trans, args=(transaction2[0],))
thread4 = threading.Thread(target=run_trans, args=(transaction2[1],))

thread3.start()
thread4.start()

thread3.join()
thread4.join()

transaction3 = []
transaction3.append(Transaction())
transaction3.append(Transaction())
transaction3[0].add_query(query.update, grades_table, 1, *[1, 22, 33, 44, 55])
transaction3[0].add_query(query.update, grades_table, 6, *[6, 77, 88, 99, 100])
transaction3[1].add_query(query.update, grades_table, 11, *[11, 2, 3, 4, 5])
transaction3[1].add_query(query.update, grades_table, 16, *[16, 6, 7, 8, 9])

thread5 = threading.Thread(target=run_trans, args=(transaction3[0],))
thread6 = threading.Thread(target=run_trans, args=(transaction3[1],))

thread5.start()
thread6.start()

thread5.join()
thread6.join()


print(query.select(1, 0, [1, 1, 1, 1, 1]))
print(query.select(6, 0, [1, 1, 1, 1, 1]))
print(query.select(11, 0, [1, 1, 1, 1, 1]))
print(query.select(16, 0, [1, 1, 1, 1, 1]))

db.close()
