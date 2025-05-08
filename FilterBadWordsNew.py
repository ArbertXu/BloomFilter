import base64
import numpy as np
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import hashlib
import re
sc = SparkContext(appName="BloomFilterStreaming")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 9999)
with open("/home/arbert_xu/BloomFilter.txt", 'r') as f:
    bloomFilter64 = f.read().strip()
decodedBits = []
bloomBits = base64.b64decode(bloomFilter64)
for byte in bloomBits:
    bits = bin(byte)[2:].zfill(8)
    decodedBits.extend([int(bit) for bit in bits])
bitArraySize = 1900
decodedBits = decodedBits[:bitArraySize]
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
        if decodedBits[hashFunc] == 0:
            return False
    return True
def cleanWord(word):
    return re.sub(r'\W+', '', word.lower())
def checkSentence(sentence):
    words = sentence.split()
    for word in words:
        if isBadWord(cleanWord(word)):
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