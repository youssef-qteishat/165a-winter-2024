from lstore.db import Database
from lstore.query import Query

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
query.insert(*[1, 2, 3, 4, 5])
db.close()
db.open('./ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)
print(query.select(1, 0, [1, 1, 1, 1, 1]))
db.close()
