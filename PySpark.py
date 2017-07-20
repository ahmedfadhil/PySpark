from pyspark import SparkContext

sc = SparkContext()
textFile = sc.textFile('example.txt')
textFile.count()
textFile.first()

secfind = textFile.filter(lambda line: 'second' in line)
secfind.collect
words = textFile.map(lambda line: line.splite())
words.collect()

words = textFile.flatmap(lambda line: line.splite()).collect()
services = sc.textFile('services.txt')
services.take(2)
services.map(lambda line: line.splite()).take(3)
clean = services.map(lambda line: line[1:] if line[0] == '#' else line).collect()

clean = clean.map(lambda line: line.splite())
clean.collect()
pairs = clean.map(lambda lst: (lst[3], lst[-1]))

rekey = pairs.reduceByKey(lambda amt1, amt2: float(amt1) + float(amt2))

rekey.collect()

# Grab the data
step1 = clean.map(lambda lst: (lst[3], lst[1]))
# Reduce by key
step2 = step1.reduceByKey(lambda amt1, amt2: (amt1 + amt2))
# Get rid of State and Amount title
step3 = step2.filter(lambda x: not x[0] == 'State')
# Sort result by amount
step4 = step3.sortBy(lambda stAmount: stAmount[1], ascending=False)
# Action
step4.collect()

x = ["ID", "State", "Amount"]


def func1(lst):
    return lst[-1]


def func2(id_st_amt):
    # unpack values
    (id, st, amt) = id_st_amt
    return amt
