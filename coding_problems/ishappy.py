# the below code successfully passed
def isHappy(n):
    """
    :type n: int
    :rtype: bool
    """
    origin = []
    while n != 1:
        sum = 0
        for i in str(n):
            sum += int(i)*int(i)
        if ( sum in origin ):
            print(origin)
            return False
        origin.append(sum)
        n = sum
    return True

print(isHappy( 111 ))