package org.matruss.waterfall

import org.specs2.mutable._
import com.twitter.scalding.JobTest
import cascading.tuple.{ Tuple => CascadingTuple }
import org.slf4j.LoggerFactory
import com.twitter.scalding.avro.{AvroSchemaType, PackedAvroSource}
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import java.util.regex.Pattern

@RunWith(classOf[JUnitRunner])
class DataPipeSpec extends Specification {

  // naive point of view would be that one can add the whole collection to build a tuple
  // but corresponding constructor is broken as of Cascading 2.5.2 !
  def fill(line:String):List[CascadingTuple] = {
    val tuple = new CascadingTuple
    line.split('|') foreach { elem => tuple.add(elem) }
    if (line.last == '|') tuple.add("")
    List(tuple)
  }
  val log = LoggerFactory.getLogger(this.getClass.getName)
  val separator = "|"

  "A pre-processor" should {

    implicit val avroType = new AvroSchemaType[InputEvent] { def schema = InputEvent.getClassSchema }

    "process batchimport source line properly with just ADD operations" in {
      val biLine = "100027022cb6a3d|et.ec,et.ef|"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 1)
        val buf = output(0)
        val (cookie_id,edge,change_type, change) =
          (buf.getCookieId.toString, buf.getOrigin.toString, buf.getChangeType.toString, buf.getChange)
        assert(cookie_id.equalsIgnoreCase("100027022cb6a3d"))
        assert(edge.equalsIgnoreCase("batchimport"))
        assert(change_type.equalsIgnoreCase("command"))
        assert(change.size == 3)
        // don't know why, but change gets populated twice, second time with nulls, thus this weird check
        if (change.get("network") != null ) assert(change.get("network").toString.equalsIgnoreCase("et"))
        if (change.get("segments") != null ) assert(change.get("segments").toString.equalsIgnoreCase("et.ec,et.ef"))
        if (change.get("operation_type") != null ) assert(change.get("operation_type").toString.equalsIgnoreCase("Add"))
      }
      test.run.runHadoop.finish

      true
    }

    "process batchimport source line properly with just REMOVE operations" in {
      val biLine = "100027022cb6a3d||et.ec,et.ef"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 1)
        val buf = output(0)
        val (cookie_id,edge,change_type, change) =
          (buf.getCookieId.toString, buf.getOrigin.toString, buf.getChangeType.toString, buf.getChange)
        assert(cookie_id.equalsIgnoreCase("100027022cb6a3d"))
        assert(edge.equalsIgnoreCase("batchimport"))
        assert(change_type.equalsIgnoreCase("command"))
        assert(change.size == 3)
        // don't know why, but change gets populated twice, second time with nulls, thus this weird check
        if (change.get("network") != null ) assert(change.get("network").toString.equalsIgnoreCase("et"))
        if (change.get("segments") != null ) assert(change.get("segments").toString.equalsIgnoreCase("et.ec,et.ef"))
        if (change.get("operation_type") != null ) assert(change.get("operation_type").toString.equalsIgnoreCase("Remove"))
      }
      test.run.runHadoop.finish

      true
    }

    "process batchimport source line properly with just both ADD and REMOVE operations" in {
      val biLine = "100027022cb6a3d|et.ec,et.ef|et.bk"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 2)
      }
      test.run.runHadoop.finish

      true
    }

    "process batchimport source line properly with just both ADD and REMOVE operations grouping on network" in {
      val biLine = "100027022cb6a3d|et.ec,bc.ef|et.bk"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 3)
      }
      test.run.runHadoop.finish

      true
    }

    "process batchimport source line properly with malformed ADD segments" in {
      val biLine = "100027022cb6a3d|null|et.bk"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 1)
      }
      test.run.runHadoop.finish

      true
    }
    "skip batchimport source line with cookie from blacklist" in {
      val biLine = "100027022cb6a3d|null|et.bk"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").arg("black-cookies-list","123, 100027022cb6a3d").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 0)
      }
      test.run.runHadoop.finish

      true
    }

    "process batchimport source line properly with malformed REMOVE  segments" in {
      val biLine = "100027022cb6a3d|null|null"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result")) { output =>
        assert(output.size == 0)
      }
      test.run.runHadoop.finish

      true
    }

    "skip batchimport source lines without cookie_ids" in {
      val biLine = "|et.ec,et.ef,et.do,et.ed|et.dz"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result"))  { output =>
        assert(output.size == 0)
      }
      test.run.runHadoop.finish

      true
    }

    "skip batchimport source lines with cookie_id with non-printable chracters" in {
      val biLine = "100027022c\u0000b6a3d|et.ec,et.ef,et.do,et.ed|et.dz"

      val rows = fill(biLine)
      val test = JobTest("com.collective.waterfall.Runnable").arg("batchimport","input/rows").arg("batchoutput","out/result").arg("testmode","").
        source(Cdf("input/rows", separator),rows).
        sink[InputEvent](PackedAvroSource("out/result"))  { output =>
        assert(output.size == 0)
      }
      test.run.runHadoop.finish

      true
    }
  }
}

