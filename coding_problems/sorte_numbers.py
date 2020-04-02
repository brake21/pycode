def square_sorted(input_list):
    temp_val = []
    for i in input_list:
        new_val = i * i
        temp_val.append(new_val)

    for i in range(len(temp_val)):
        for j in range(i + 1, len(temp_val)):
            if (temp_val[i] > temp_val[j]):
                temp = temp_val[i]
                temp_val[i] = temp_val[j]
                temp_val[j] = temp

    return temp_val


print(square_sorted([-4, -1, 0, 3, 10]))