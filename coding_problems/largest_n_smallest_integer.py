list = [12, 45, 2, 41, 31, 10, 8, 6, 4]

def largets_n_smallest(list):
    maxnum = list[0]
    minnum  = list[0]
    for i in list:
        maxnum = max(maxnum, i)
        minnum = min (minnum, i)
    return maxnum, minnum

print(largets_n_smallest(list))