import random

with open('list.csv', 'w') as f:
    for i in range(0, 10000000):
        f.write(str(random.randint(0, 1000000))+"\n")
