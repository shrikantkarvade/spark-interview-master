package com.shrikant.spark.foundation

final class SparkApplicationConfigException(
                                             message: String,
                                             cause: Throwable = null
                                           ) extends RuntimeException(message, cause)