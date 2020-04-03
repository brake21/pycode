from collections import defaultdict
def solution(numbers):
    # Type your solution here
    output_list = []
    dic = defaultdict(int)
    for i in numbers:
        dic[i] += 1
    return sorted([x[0] for x in dic.items() if x[1] == 1])
