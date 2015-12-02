import sys
import time
from random import random
from operator import add
from pyspark import SparkContext

def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts)
        return result

    return timed

@timeit
def main(partitions):
    """
        Usage: pi [partitions]
    """
    sc = SparkContext(appName="PythonPi")
    n = 1000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

    count = sc.parallelize(xrange(1, n + 1), partitions).map(f).reduce(add)
    print "Pi is roughly %f" % (4.0 * count / n)

    sc.stop()
    

if __name__ == "__main__":
    i= int(sys.argv[1]) if len(sys.argv) > 1 else 2
    main(i)
    
