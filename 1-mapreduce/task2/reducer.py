import sys

current_key = None
word_sum = 0

for line in sys.stdin:
    key, article_id, count = line.strip().split('\t', 2)
    count = int(count)
    if current_key != key:
        if current_key:
            print("{}\t{}".format(current_key, word_sum))
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print("{}\t{}".format(current_key, word_sum))
