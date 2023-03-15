import sys
import random

final_string = ""
loop_max = random.randint(1, 5)

for line in sys.stdin:

    if loop_max > 1:
        spec, id1 = line.strip().split('\t', 1)
        final_string = final_string+id1+","


    if loop_max <= 1:
        spec, id1 = line.strip().split('\t', 1)
        final_string = final_string+id1
        print("{}".format(final_string))
        final_string = ""

    loop_max = loop_max - 1
    if loop_max == 0:
        loop_max = random.randint(1, 5)
