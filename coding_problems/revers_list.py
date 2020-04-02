import time
list = [1,2,3,3,3,5,5,7,8,8,9,8]


def reverse_list(list):
    count = len(list)
    reversed = []
    for each_number in list:
        reversed.append(list[count-1])
        count -= 1
    return reversed

print(reverse_list(list))