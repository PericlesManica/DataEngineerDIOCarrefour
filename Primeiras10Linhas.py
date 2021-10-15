import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc")
    words = sc.textFile("gs://bootcamp-dio-dataproc/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    wordCounts.saveAsTextFile("gs://bootcamp-dio-dataproc/resultado")
    arquivo = open('gs://bootcamp-dio-dataproc/resultado/part-00000', 'r')
    arquivo1 = open('gs://bootcamp-dio-dataproc/resultado/resultado.txt', 'w')
    for n in range(0, 10):
        arquivo1.write(arquivo.readline())
    arquivo.close()
    arquivo1.close()
