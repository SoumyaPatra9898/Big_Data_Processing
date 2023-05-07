import sys
import math
from pyspark import SparkContext
#spark-submit <your-code> <path to file> <query-word> <k> <stopword-file>

def isCorrect(word,stopWords):
    return len(word)>0 and word.isalpha() and (word not in stopWords)

def co_freq(elem):
    return [(word, 1) for word in elem]

def computePMI(p1,p2,p1p2):
    #pmi=float(-inf)
    if p1p2==0:
        return -1000000
    pmi=math.log2(p1p2/(p1*p2))
    return pmi

if __name__=='__main__':

    fileName=sys.argv[1]
    query = sys.argv[2]
    k=int(sys.argv[3])
    stopFile=sys.argv[4]


    sc=SparkContext.getOrCreate()
    #print("hi1")

    stopWords=sc.textFile(stopFile)
    stopWords=stopWords.flatMap(lambda elem: [word.lower() for word in elem]).collect()
    stopWords=list(set(stopWords))

    query=query.lower()
    file=sc.textFile(fileName)
    docs=file.flatMap(lambda elems:elems.split('\n'))
    #print("hi2")
    elems1= file.map(lambda elem: [word.lower() for word in elem.split(' ') if isCorrect(word,stopWords)]).filter(lambda elem: len(elem)>0)
    elems2 =elems1.map(lambda elem:list(set(elem)))
    #print("hi3")
    word_presence = elems2.flatMap(lambda elem: [(word,1) for word in elem])
    '''print("\n\n", "@"*100)
    print(elems2.collect())
    print("\n\n", "@"*100)'''
    #exit()
    co_word_presence = elems2.map(lambda elem:elem if query in elem else []) 
    '''print("\n\n", "@"*100)
    print(co_word_presence.collect())
    print("\n\n", "@"*100)'''
    co_word_count=co_word_presence.flatMap(lambda elem:[(word,1) for word in elem])
    #print("hi4")
    freq=word_presence.reduceByKey(lambda a,b:a+b)
    cofreq=co_word_count.reduceByKey(lambda a,b:a+b)

    mp=dict(freq.collect())

    n=elems2.count()

    if n==0:
        print('There is no document')
        exit()

    p2=mp[query]/n
    ''' print("\n\n", "@"*100)
    print(cofreq.collect())
    print("\n\n", "@"*100)'''
    ans=cofreq.map(lambda word:(word[0],computePMI(mp[word[0]]/n,p2,word[1]/n))).sortBy(lambda word:word[1]).filter(lambda word:word[1]!=-1000000).collect()
    '''print("\n\n", "@"*100)
    print(ans)
    print("\n\n", "@"*100)'''
    #ans= list(sorted(ans.items(), key=lambda item: item[1]))
    '''ans_list = []
    for k,v in ans:
        ans_list.append((k,v))'''
    posK=[]
    negK=[]
    i=0
    tot=len(ans)

    for i in range(min(k,tot)):
        if ans[i][1]<0:
            negK.append(ans[i])
        if ans[tot-i-1][1]>0:
            posK.append(ans[tot-i-1])

    final=posK+negK
    #print("\n\n", "@"*100)
    for word,score in posK:
        print(word, score)
    print('\n')
    for word,score in negK:
        print(word, score)
    #print("\n\n", "@"*100)
    #print ("hi")





