from functools import wraps

try:
    from pyspark import Broadcast, SparkConf, SparkContext, TaskContext
    from pyspark.rdd import RDD, portable_hash
    has_spark = True
except ImportError:
    RDD = None
    SparkContext = None
    TaskContext = None
    SparkConf = None
    portable_hash = None
    Broadcast = None
    
    has_spark = False



def require_spark(func):    
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not has_spark:
            raise ImportError("pyspark is required for function: %s" % func.__name__)
        return func(*args, **kwargs)
    
    return wrapper
