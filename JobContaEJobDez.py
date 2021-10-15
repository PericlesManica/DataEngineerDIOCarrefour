import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark - Desafio Dataproc")
    words = sc.textFile("gs://bootcamp-dio-dataproc/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False) 
    texto = wordCounts.take(10)   
    text10 = sc.parallelize(texto)
    wordCounts.saveAsTextFile("gs://bootcamp-dio-dataproc/resultado")
    text10.saveAsTextFile("gs://bootcamp-dio-dataproc/result10")
