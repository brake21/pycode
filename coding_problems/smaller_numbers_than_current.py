def smallerNumbersThanCurrent(nums):
    """
    :type nums: List[int]
    :rtype: List[int]
    """
    output_lst = []
    for i in range(len(nums)):
        count = 0
        for j in range(len(nums)):
            if ( i != j ):
                if ( nums[j] < nums[i] ):
                    count += 1
        output_lst.append(count)
    return output_lst

