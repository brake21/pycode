input ='AAABCCDDDD'

from collections import defaultdict

def string_compression(input):
    output = defaultdict(int)
    for char in input:
        output[char] = output[char] + 1
    return output

print(string_compression(input))