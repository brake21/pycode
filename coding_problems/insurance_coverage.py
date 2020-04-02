# Problem statement
# Scheduling on-call rotations can be hard - sometimes people have commitments, like family, video games, or sleep. Given an existing coverage schedule (i.e. day ranges for which we already have coverage) and a new engineer's availability, return the updated coverage schedule

# rob = [20, 25]  # [1,30]
coverage = [[4, 15], [18, 21], [28, 30], [35, 200], [20, 25]]
# new_coverage = add_engineer(rob, coverage)
# new_coverage == [[4, 15], [18, 25], [28, 30], [35, 200]]
#
# inp1 = rob[0]
# inp2 = rob[1]
#
# def add_engineer(rob,coverage):
#     output = coverage
#     if (max(inp1, inp2) < min(coverage[0]) or min(inp1, inp2) > max(coverage[-1]) ):
#         output.append(rob)
#     else:
#         for i in range(0, len(coverage)):
#             prev = coverage[i][-1]
#             curr = coverage[i + 1][0]

def merge_overlaps(input_list):
    input_list.sort()
    output_list = []
    cleared_indexes = [1]
    for i in range(len(input_list)):
        count = 0
        value = input_list[i][1]
        if ( i == max(cleared_indexes) ):
            continue
        for j in range(i+1, len(input_list)):
            if ( input_list[j][0] <= value <= input_list[j][1]):
                new_value = [ input_list[i][0], input_list[j][1] ]
                output_list.append(new_value)
                cleared_indexes.extend([i,j])
                count += 1
        if ( count == 0 ):
            output_list.append(input_list[i])
    return output_list

print(merge_overlaps(coverage ))