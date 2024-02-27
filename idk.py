from lstore.table import Table
from lstore.query import Query
from lstore.db import Database

db = Database()
db.open('./ECS165')
table = grades_table = db.get_table('Grades')
q = Query(table)

#print("select!!!", q.select_version(92106429, 0, [1,1,1,1,1], 0)[0].columns)
print(table.page_directory)
print("read", table.read_record(29, 0))
db.close()
exit()
print("merged...")
table.mergetest()
print("mergee!")
print("select2", q.select_version(1, 0, [1,1,1,1,1], -3)[0].columns)
print("select2", q.select_version(1, 0, [1,1,1,1,1], 0)[0].columns)

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
