"""Utilities for Apache Spark."""

from kgdata.spark.extended_rdd import ExtendedRDD, DatasetSignature
from kgdata.spark.rdd_alike import SparkLikeInterface
from kgdata.spark.common import *

__all__ = [
    "DatasetSignature",
    "ExtendedRDD",
    "SparkLikeInterface",
    "does_result_dir_exist",
    "get_spark_context",
]