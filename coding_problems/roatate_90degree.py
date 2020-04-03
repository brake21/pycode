def solution(matrix):
    horz = [len(matrix) - 1, 0]
    out_matrix = []
    for i in range(len(matrix)):
        buff = horz
        temp_list = []
        for j in range(len(matrix[i])):
            x = buff[0]
            y = buff[1]
            new_x = abs(i + x)
            new_y = abs(j - y)
            new_val = matrix[new_x][new_y]
            temp_list.append(new_val)
            buff = [x - 1, y + 1]
        out_matrix.append(temp_list)
        horz = [ horz[0]-1, horz[1]+1 ]
    print(out_matrix)
matrix = [[1,2,3],
          [4,5,6],
          [7,8,9]]

output = [[7,4,1],
          [8,5,2],
          [9,6,3]]

solution(matrix)
