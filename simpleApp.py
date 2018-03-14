from pyspark.sql import SparkSession

logFile="/home/wht/software/spark/README.md"
spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
logData=spark.read.text(logFile).cathe()

numAs=logData.filter(logData.value/contains('a')).count()
numBs=logData.filter(logData.value/contains('b')).count()

print("Lines with a: %i,lines with b: %i"(numAs,numBs))

spark.stop()