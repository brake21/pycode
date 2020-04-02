def sum_indexes(target, arr):
    output = []
    for i in range(len(arr)):
        diff = target - arr[i]
        if ( diff in arr ):
            output.append(i)
    return output

print(sum_indexes(7, [1,8,33,6]))


def ispalindrome(string):
    for i in range(0, len(string) // 2):
        if string[i] != string[len(string) - i - 1]:
            return False
    return True

print(ispalindrome("pcopc"))