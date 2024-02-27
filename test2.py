from lstore.table import Table
from lstore.query import Query
from lstore.db import Database
db = Database()
db.open('./okbuddy')
table = db.create_table("Student Grades", 5, 0)
q = Query(table)

rid1 = table.insert_record([1, 2, 3, 4, 5])
rid2 = table.insert_record([6, 5, 8, 9, 10])
table.update_record([None, 56, None, None, None], rid1)
table.update_record([None, None, 23, None, None], rid1)
print("read", table.read_record(rid1, 0))
#print(table.read_record(rid2, 0))
print("read2", table.read_record(rid1, 0))
print("select!!!", q.select_version(1, 0, [1,1,1,1,1], -1)[0].columns)

print(rid1, rid2)
table.mergetest()
#table.mergetest()

print("read2", table.read_record(rid1, 0))
#print("___________________________-")
#table.update_record([None, 47, 234, None, 1], rid2)
#print(table.read_record(rid1, 0))
#print(q.select(56, 1, [1, 1, 1, 1, 1])[0].columns)
#print("_______________________-")
#q.insert(43, 567, 65, 5)
# first 4 columns are meta data, last 5 are the contents
#print(table.read_record(rid1), rid1)
#print(table.read_record(rid2), rid2)
#print(table.read_record(4))
#rid3 = q.insert(2, 77, 89, 100, 21)
#rid4 = q.insert(3, 5, 55, 13, 23)
#rid5 = q.insert(4, 5, 23, 2, 4)
#rid6 = q.insert(4234, 5, 12, 56, 88)
#print(q.sum(4, 8, 0))
#print(table.read_record(rid3))
#print(q.select(43, 0, [1, 1, 1, 1, 0]))
#print(q.select(5, 1, [1,1,1,1,1])[0].columns)
print("select2", q.select_version(1, 0, [1,1,1,1,1], -1)[0].columns)
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
