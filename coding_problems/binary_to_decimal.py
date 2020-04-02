# Definition for singly-linked list.
# class ListNode(object):
#     def __init__(self, x):
#         self.val = x
#         self.next = None

class Solution(object):
    def getDecimalValue(self, head):
        """
        :type head: ListNode
        :rtype: int
        """
        """ 1*2**(len(head))"""
        # print(dir(head))
        power = self.getCountRec(head) - 1
        if (not head):
            return 0
        else:
            return (head.val * 2 ** power + self.getDecimalValue(head.next))

    def getCountRec(self, node):
        if (not node):  # Base case
            return 0
        else:
            return 1 + self.getCountRec(node.next)