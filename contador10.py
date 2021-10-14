import sys
from pyspark import SparkContext, SparkConf
from collections import Counter
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc")
    words = sc.textFile("gs://bootcamp-dio-dataproc/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    c = Counter(wordCounts)
    top10 = c.most_common(10)    
    wordCounts.saveAsTextFile("gs://bootcamp-dio-dataproc/resultado")
    top10.saveAsTextFile("gs://bootcamp-dio-dataproc/resultado10")
