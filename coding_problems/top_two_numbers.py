list = [1,2,99,99,3,3,3,5,5,7,8,8,9,8]

def repeating_elements(list):
    out = { first_top : list[0], second_top: list[1] }
    for i in list[2:]:
        if ( i > out['first_top'] and  i > out['second_top'] ):
            out['second_top'] = out['first_top']
            out['first_top'] = i

        elif (i < out['first_top'] and  i > out['second_top'] ):
            out['second_top'] = i
        else:
            pass
    ts = time.time()
    temp = []
    repeating_number = []
    for i in list:
        if ( i not in temp ):
            temp.append(i)
        elif ( (i in temp) and (i not in repeating_number) ):
            repeating_number.append(i)
    print(temp)
    print(repeating_number)
    print(f"time took to process the records: {time.time() - ts}")


