from table import Table


table = Table("Student Grades", 5, 0)

rid1 = table.insert_record([1, 2, 3, 4, 5])
rid2 = table.insert_record([6, 7, 8, 9, 10])
table.update_record([None, 56, None, None, None], rid1)
table.update_record([253, None, None, None, None], rid1)
table.update_record([42, None, 234, None, 1], rid2)

# first 4 columns are meta data, last 5 are the contents
print(table.read_record(rid1))
print(table.read_record(rid2))
