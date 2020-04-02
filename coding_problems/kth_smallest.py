from queue import PriorityQueue
list = [8,3,0,5,1,4,9]


def kth_smallest(nums, k):

    l = PriorityQueue(k+1)
    for num in nums:
        l.put(num)
        if l.full():
            l.get()
    return l.get()

print(kth_smallest(list,7))