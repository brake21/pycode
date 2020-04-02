N=8
count=1
hops=0
while count <= 10**4:
    if ( i+count > i*count ):
        if ( i+count == N ):
            hops = hops + 1
            break
        else:
            hops = hops + 1
    else:
        if ( i*count == N ):
            hops = hops + 1
            break
        else:
            hops = hops + 1
print(hops)