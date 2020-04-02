list = [12, 45, 2, 41, 31, 10, 8, 6, 4, 33]

def sum_pair(sum, list):
    x=len(list)
    lst=[]
    for i in range(x):
        k = i +1
        for j in range(k, x):
            if ( sum == (list[i]+list[j]) ):
                lst.append(set([list[i], list[j] ]))
    return lst

def sum_pair_1(sum, list):
    x=len(list)
    lst={}
    for i in range(x):
        if list[i] in lst:
            print(lst[list[i]], list[i])
            return True
        else:
            lst[sum - list[i]] = list[i]
    return False

sum_pair_1(43, list)
print(sum_pair(43, list))