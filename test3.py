from lstore.db import Database
from lstore.query import Query

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
db.close()