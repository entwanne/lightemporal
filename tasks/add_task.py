import sys

from .shared import toto
from .queue import Q


Q.put(toto, sys.argv[1] if len(sys.argv) > 1 else 'hello', int(sys.argv[2]) if len(sys.argv) > 2 else 3)
