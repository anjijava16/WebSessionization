package com.websessionization.sparkutil

import org.apache.spark.AccumulatorParam

/**
 * Created by hardik on 19/08/15.
 */

/** *
  * Custom Accumulator: Long Type AccumulatorParam,
  * Spark By Default creates only for Init type.
  *
  */
object LongAccumulatorParam extends AccumulatorParam[Long] {

  override def addInPlace(r1: Long, r2: Long): Long = {
    r1+r2
  }

  override def zero(initialValue: Long): Long = {
    0L
  }

}

