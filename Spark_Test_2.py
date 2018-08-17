from pyspark import SparkConf, SparkContext

#Basic Setup
conf = SparkConf().setMaster("local[*]").setAppName("Superheros")
sc = SparkContext(conf=conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split("\"")
    return (int(fields[0]), fields[1].encode("utf8"))

#Load data into RDD
names = sc.textFile("/Users/mattjones/Desktop/SparkCourse/Marvel-Names.txt")
namesRDD = names.map(parseNames)


lines = sc.textFile("/Users/mattjones/Desktop/SparkCourse/Marvel-Graph.txt")
pairings = lines.map(countCoOccurences)
pairings.saveAsTextFile("Pairings")

totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
totalFriendsByCharacter.saveAsTextFile("Reduced")

flipped = totalFriendsByCharacter.map(lambda x: (x[1], x[0]))

mostPopular = flipped.max()
mostPopularName = namesRDD.lookup(mostPopular[1])[0]
print(mostPopularName)
print(mostPopular[0])