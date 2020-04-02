import time
list = [1,2,99,99,3,3,3,5,5,7,8,8,9,8]


def repeating_elements(list):
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


def first_repeating_element(list):
    temp = []
    repeating_number = []
    for i in list:
        if (i not in temp):
            temp.append(i)
        elif ((i in temp) and (i not in repeating_number)):
            repeating_number.append(i)
            break
    print(repeating_number)

first_repeating_element(list)
# repeating_elements(list)