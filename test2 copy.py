from lstore.table import Table
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./weee')
table = db.get_table("Student Grades")
q = Query(table)
#print("select!!!", q.select_version(6, 0, [1,1,1,1,1], 0)[0].columns)

print("select!!!", q.select_version(1, 0, [1,1,1,1,1], -1)[0].columns)

print("merged...")
#table.mergetest()
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
