from pyspark.sql import SparkSession
from pyspark.sql import Row

def loadMovieNames():
    movieNames = {}
    with open("/Users/mattjones/Desktop/SparkCourse/ml-100k/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]

    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate() #Fix this
nameDict = loadMovieNames()

lines = spark.sparkContext.textFile("file:///Users/mattjones/Desktop/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))

movieDataset = spark.createDataFrame(movies)
topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()
topMovieIDs.show()
top10 = topMovieIDs.take(10)

print("\n")
for result in top10:
    print("Key: {}, Value: {}".format(nameDict[result[0]], result[1]))

spark.stop()
