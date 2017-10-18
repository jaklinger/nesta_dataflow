import sys
if len(sys.argv) == 4:
    
    find=sys.argv[1]
    replace=sys.argv[2]
    
    with open(sys.argv[3]) as f:
        text = f.read()
        text = text.replace(find,replace)

    with open(sys.argv[3],"w") as f:
        f.write(text)
