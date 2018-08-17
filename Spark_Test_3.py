from pyspark import SparkConf, SparkContext

#Basic Setup
conf = SparkConf().setMaster("local[*]").setAppName("Superheros") #Use every core available
sc = SparkContext(conf=conf)

#Now load the RDD
def parseLine(line):
    items = line.split(",")
    return (items[0], items[1])

prices = sc.textFile("/Users/mattjones/Desktop/SparkCourse/SP500.csv")
pricesRDD = prices.map(parseLine)

#Filter to 2018 and 2017 only
filteredRDD = pricesRDD.filter(lambda x: x[0][0:4] in ["2016", "2017", "2018"])
count = filteredRDD.count()
print(count)

flipped = filteredRDD.map(lambda x: (x[1], x[0]))
max = flipped.max()
print(max)

min = flipped.min()
print(min)

