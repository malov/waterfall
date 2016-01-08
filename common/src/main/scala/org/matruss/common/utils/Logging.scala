package org.matruss.common.utils

import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

trait Logging {
  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  def setLogLevel(level : Level) = Logger.getRootLogger.setLevel(level)
}
