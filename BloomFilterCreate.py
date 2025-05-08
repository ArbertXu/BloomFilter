import hashlib
import base64
import urllib.request
import re
affinityURL = "https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt"
affinityLocal = "AFINN-en-165.txt"
urllib.request.urlretrieve(affinityURL, affinityLocal)
badWords = []
with open(affinityLocal, "r", encoding="utf-8") as affinityFile:
    for line in affinityFile:
        word, score = line.strip().split("\t")
        if int(score) <= -4:
            badWords.append(word.lower())
print(badWords)
print(f"length of bad words: {len(badWords)}")
bitArraySize = 1900
HashFunctions = 20
# Using formula online
bitArray = [0] * bitArraySize
def getHash(word):
    hashes = []
    for num in range(0, HashFunctions):
        hashFunc = hashlib.md5((word + str(num)).encode('utf-8')).hexdigest()
        index = int(hashFunc, 16) % bitArraySize
        hashes.append(index)
    return hashes
def cleanWord(word):
    return re.sub(r'\W+', '', word.lower())
for word in badWords:
    for index in getHash(cleanWord(word)):
        bitArray[index] = 1
bitString = ''.join(str(bit) for bit in bitArray)
bytesArr = int(bitString, 2).to_bytes((len(bitString) + 7) // 8, byteorder='big')
base64Encoded = base64.b64encode(bytesArr).decode('utf-8')

with open("BloomFilter.txt", "w", encoding="utf-8") as outputFile:
    outputFile.write(base64Encoded)
print(f"Base64 encoded: {base64Encoded}")


print("SuccessYAY!")