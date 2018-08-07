from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("/Users/mattjones/Downloads/ml-100k/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

loadMovieNames()

#Basic Setup
conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames()) #Broadcast the data to all nodes

#Data Transformation
lines = sc.textFile("file:///Users/mattjones/Downloads/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1)) #Create a tuple
movieCounts = movies.reduceByKey(lambda x, y: x + y)

#Reverse key and value
flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda x: (nameDict.value[x[1]], x[0])) #Read back from the broadcast variable

results = sortedMoviesWithNames.collect()

for _ in results:
    print(_)

