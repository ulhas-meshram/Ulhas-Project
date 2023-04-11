list1 = [1,2,3,4,5,6,7,8,9,10,11,5,3,1]
list2 = []
def remove(list1):
    for i in list1:
        if i not in list2:
            list2.append(i)
    return list2
print(remove(list1))
# -----------------------------------------------------------------------------

str1 = "ulhas meshram"
def reverse(str1):
    str2=""
    for i in str1:
        str2=i+str2
    return str2
print(reverse(str1))

# --------------------------------------------------------------

sentence1 = "ulhas meshram ashish meshram"
sentence2 = sentence1.split(' ')
sentence2.reverse()
print(sentence2)
outputstring=" ".join(sentence2)
print(outputstring)

# ------------------------------------------------------------------------------

saa="Ulhas Ashokrao Meshram Ashish Ashokrao Meshram"
wordss=saa.split()
for x in wordss:
    caa=list(x)
    caa.reverse()
    outss = "".join(caa)
    print(outss,end=" ")
