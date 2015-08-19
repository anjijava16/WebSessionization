/**
 * Created by hardik on 19/08/15.
 */
package com.websessionization.config

import com.typesafe.config._

object SessionConfig {
  //load the conf
   val config = ConfigFactory.load(getClass().getClassLoader)

}