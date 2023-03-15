import sys
import re

counter = 0

for line in sys.stdin:
    counter = counter + 1
    #print("-----Line number: {} -----".format(counter))
    #print("{}".format(line))

    words = line.strip().split()
    article_id = words[0]
    words_set = set()
    for word in words:
        if words.index(word) > 0:
            word = re.sub(r'[^\w\s\']', '', word.lower())
            #если слово уже встречалось в этом документе, не выводим его
            #все, кроме первого элемента этого массива, забить в set

            #запоминаем размер set до добавления элемента
            set_len_old = len(words_set)
            #добавляем элемент в set
            words_set.add(word)
            #если размер set изменился (увеличился), то выводим элемент
            if len(words_set) == set_len_old + 1:
                if len(word) > 0:
                    print("{}\t{}\t{}".format(word, article_id, 1))
                    #если размер set не изменился, то не выводим
