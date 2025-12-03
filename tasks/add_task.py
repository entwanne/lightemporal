import sys

from .shared import toto
from .queue import ENV

word = sys.argv[1] if len(sys.argv) > 1 else 'hello'
count = int(sys.argv[2]) if len(sys.argv) > 2 else 3

Q = ENV['Q']
print(Q.execute(toto, word, count))
