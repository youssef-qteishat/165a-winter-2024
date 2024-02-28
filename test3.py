from lstore.db import Database
from lstore.query import Query
import cProfile
from random import choice, randrange

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
profiler = cProfile.Profile()

profiler.enable()
for i in range(0, 1000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
profiler.disable()

profiler.print_stats(sort='cumulative')


# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

profiler.enable()
for i in range(0, 1000):
    query.update(choice(keys), *(choice(update_cols)))
profiler.disable()

profiler.print_stats(sort='cumulative')

db.close()