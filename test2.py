from lstore.table import Table
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./weee')
table = db.create_table("Student Grades", 5, 0)
q = Query(table)

rid1 = table.insert_record([1, 2, 3, 4, 5])
rid2 = table.insert_record([6, 5, 8, 9, 10])
print("????")

table.update_record([None, 56, None, None, None], rid1)
table.update_record([None, None, 23, None, None], rid1)
table.update_record([None, None, None, 55, None], rid1)
print("????")
print("read", table.read_record(rid1, 0))
print("select!!!", q.select_version(1, 0, [1,1,1,1,1], -1)[0].columns)

print("merged...")
table.mergetest()
print("mergee!")
print("select2", q.select_version(1, 0, [1,1,1,1,1], -1)[0].columns)
print("select2", q.select_version(1, 0, [1,1,1,1,1], 0)[0].columns)
print("select2", q.select_version(1, 0, [1,1,1,1,1], -4)[0].columns)

db.close()
exit()
print("????")

print("Deleting record...")
table.delete_record(rid1)
try :
    table.read_record(rid1)
except Exception as e:
    print(e)
    print("Could not read deleted record, this is the correct behavior")

rid1 = table.insert_record([11, 12, 13, 14, 15])
print(table.read_record(rid1))
