#! /usr/bin/env bash

data="data/ids.txt"
#cat $data | python3 ./mapper.py | sort | python3 ./reducer.py | sort -k2rn | head -50
cat $data | python3 ./mapper.py | sort | python3 ./reducer.py | head -50
