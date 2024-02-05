from table import Table


table = Table("Student Grades", 5, 0)

rid1 = table.insert_record([1, 2, 3, 4, 5])
rid2 = table.insert_record([6, 7, 8, 9, 10])

# first 4 columns are meta data, last 5 are the contents
print(table.read_record(rid1))
print(table.read_record(rid2))