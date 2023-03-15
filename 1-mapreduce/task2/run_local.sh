data="data/articles_sample2.txt"
cat $data | python3 ./mapper.py | sort | python3 ./reducer.py | sort -k2rn | head -10
