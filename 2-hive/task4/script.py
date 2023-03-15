import sys

for line in sys.stdin:
    parts = line.split()
    host = parts[2]
    new_host = host.rsplit(".", 1)[0] + ".com/" + host.split("/")[-1]
    line = parts[0] + "\t" + parts[1] + "\t" + new_host + "\t" + "\t".join(parts[3:])
    print(line)
