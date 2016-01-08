package org.matruss.waterfall.util

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConversionsSpec extends Specification {

  "A covnersions" should {

    "parse original datatime with UTC at the end" in {
      val TimestampString(timestamp) = "2012-02-23 00:58:03 UTC"
      assert(timestamp == 1329958683000l)
      true
    }

    "parse  datatime  with timezone diff format" in {
      val TimestampString(timestamp) = "2012-02-23 00:58:03+00:00"
      assert(timestamp == 1329958683000l)
      true
    }
  }

}
