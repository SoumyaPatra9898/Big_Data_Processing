import sys
import random

#Disjoint set find operation 
def find(dsu,val):
    if dsu[val]!=val:
        dsu[val]=find(dsu,dsu[val])
    return dsu[val]

#Disjoint set union operation 
def union(com,s1,s2):
    p1=find(com,s1)
    p2=find(com,s2)
    dsu[p1]=p2

source=[] #source node
dest=[] #dest node
nodes=set() #set of unique nodes
id=dict() #unique value starting from 0 for each node 

#reading file and getting edges into source and destinantion
fileName=sys.argv[1]
file1=open(fileName,'r')
lines = file1.readlines()

for item in lines:
    if item=='\n':
       break
    s,d=item.split()
    source.append(int(s))
    dest.append(int(d))
    nodes.add(int(s))
    nodes.add(int(d))

numVertices=len(nodes)
numEdges=len(source)
v=numVertices

#indexes shuffling for later use
index=[] 
for i in range(numEdges):
    index.append(i)
random.shuffle(index)

#assigning id to each node value
i=0
for item in nodes:
    id[item]=i
    i+=1

dsu=[] #list of parent for each node id
for i in range(numVertices):
    dsu.append(i)

#algorithm to find 2 communities
i=0
while v>2:
    ind= index[i]
    i+=1
    p1=find(dsu,id[source[ind]])
    p2=find(dsu,id[dest[ind]])
    if p1!=p2:
        v-=1
        union(dsu,p1,p2)

#value of mincut
mincut=0
for i in range(numEdges):
    p1=find(dsu,id[source[i]])
    p2=find(dsu,id[dest[i]])
    if p1!=p2:
        mincut+=1

print("The value of probable mincut is: ",mincut)
print("Community list:")

#printing node values and community 
i=1
value={}
for item in nodes:
    community=find(dsu,id[item])
    if value.get(community)==None:
        value[community]=i
        i+=1
    print(item,value[community])