import random

gen = None

def init_gen(seed):
    global gen
    gen = random.Random(seed)
