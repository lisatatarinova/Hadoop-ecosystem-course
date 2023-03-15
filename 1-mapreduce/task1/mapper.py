import sys
import re
import random


for line in sys.stdin:
    ids = line.strip().split()
    for id1 in ids:
        print("{}\t{}".format(random.randint(1, 360), id1))
        #comment
