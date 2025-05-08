import base64
import numpy as np
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import hashlib
sc = SparkContext(appName="BloomFilterStreaming")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 9999)

with open("/home/arbert_xu/BloomFilter.txt", 'r') as f:
    bloomFilter64 = f.read().strip()
bloomBits = base64.b64decode(bloomFilter64)
bitArr = np.frombuffer(bloomBits, dtype=np.uint8)

bitArraySize = 1900
HashFunctions = 20
def getHash(word):
    hashes = []
    for num in range(0, HashFunctions):
        hashFunc = hashlib.md5((word + str(num)).encode('utf-8')).hexdigest()
        index = int(hashFunc, 16) % bitArraySize
        hashes.append(index)
    return hashes
def isBadWord(word):
    for hashFunc in getHash(word.lower()):
        if bitArr[hashFunc] == 1:
            print("Found bad word")
            print(word)
            return True
    return False

def checkSentence(sentence):
    words = sentence.split()
    for word in words:
        if isBadWord(word):
            return False
    return True
def FilterSentences(rdd):
    if rdd.isEmpty():
        print("Empty RDD")
    if not rdd.isEmpty():
        sentences = rdd.collect()
        for sentence in sentences:
            cleanSentence = checkSentence(sentence)
            if cleanSentence:
                print(sentence)

lines.foreachRDD(FilterSentences)
ssc.start()
ssc.awaitTermination()