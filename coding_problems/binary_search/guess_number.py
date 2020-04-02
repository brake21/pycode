# The guess API is already defined for you.
# @param num, your guess
# @return -1 if my number is lower, 1 if my number is higher, otherwise return 0
# def guess(num):
class Solution(object):
    def guessNumber(self, n):
        """
        :type n: int
        :rtype: int
        """
        l,r = 0, n
        while l <= r:
            m = (l + r) // 2
            x = guess(m)
            if ( x == -1):
                l,r = l,m-1
            elif ( x == 1 ):
                l,r = m+1,r
            else:
                return m