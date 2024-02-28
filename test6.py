from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}
records[1] = [1, 1, 1, 1, 1]
records[2] = [1, 2, 2, 2, 2]
records[3] = [2, 3, 3, 3, 3]
records[4] = [1, 2, 2, 2, 2]
query.insert(*records[1])
# Test if correct columns are returned 
result = query.select(1, 0, [1,0,1,0,0])
print(len(result))
print(result[0].columns)
if len(result) == 1 and len(result[0].columns) == 2 and result[0].columns[1] == records[1][2]:
    print("[0] pass")
elif len(result) == 1 and result[0].columns[0] == 1 and result[0].columns[2] == 1 and\
    result[0].columns[3] == None and result[0].columns[4] == None and result[0].columns[1] == None:
    print("[0] pass")
# Test if insertion with existing primary_key is not allowed
query.insert(*records[2])
result = query.select(1, 0, [1,1,1,1,1])
print(len(result))
print(0, result[0].columns, records[1])
if len(result) == 1 and len(result[0].columns) == 5 and result[0].columns[1] == records[1][1]\
    and result[0].columns[2] == records[1][2] and result[0].columns[3] == records[1][3]\
    and result[0].columns[4] == records[1][4]:
    print("[1] pass")
result = query.sum(1, 1, 1)
if result == 1:
    print("[2] pass")
# Test if updated record with existing primary_key is not allowed
query.insert(*records[3])
query.update(2, *records[4])
try:
    result = query.select(1, 0, [1,0,1,0,0])
    print(1, result[0].columns, records[1])
    if len(result) == 1 and len(result[0].columns) == 2 and result[0].columns[1] == records[1][2]:
        print("[3] pass")
    elif len(result) == 1 and result[0].columns[0] == 1 and result[0].columns[2] == 1 and\
    result[0].columns[3] == None and result[0].columns[4] == None and result[0].columns[1] == None:
        print("[3] pass")
except Exception as e:
    print("Something went wrong during update")
    print(e)
result = query.select(2, 0, [1,1,1,1,1])
print(len(result))
if len(result) != 0:
    print(2, result[0].columns, records[3])
if len(result) == 1 and len(result[0].columns) == 5 and result[0].columns[1] == records[3][1]\
    and result[0].columns[2] == records[3][2] and result[0].columns[3] == records[3][3]\
    and result[0].columns[4] == records[3][4]:
    print("[4] pass")

db.close()