package org.matruss.waterfall


import collection.mutable.ListBuffer
import collection.JavaConversions._
import collection.mutable.Map
import org.apache.commons.lang.StringUtils
import com.twitter.scalding._
import cascading.flow.FlowDef
import com.twitter.scalding.avro.PackedAvroSource
import TDsl._

object DataPipe {

  val schema = List('id, 'feature_add, 'feature_remove)
  val projection = List('id, 'feature_add, 'feature_remove)

  protected val EdgeId = "data"
}

class DataPipe(override val feed:Feed, implicit val mode:Mode, implicit val flowDef:FlowDef)
  extends GenericPipe(feed) with Logging with CommandEventType {

  type InputSchema = (String,String,String)
  override protected val separator = "|"

  def run(implicit uid: UniqueID):TypedPipe[InputEvent] = {
    val pipe = getPipe.toTypedPipe[InputSchema](projection)

    pipe.filter(screen).
      flatMap[InputEvent](parse).
      write( PackedAvroSource[InputEvent](feed.output) )
  }

  def schema = DataPipe.schema
  def projection = DataPipe.projection

  override def screen(implicit uid: UniqueID) = {record =>
    val (id, feature_add, feature_remove) = record
    val goodId = id != null && id.length > 0 && StringUtils.isAsciiPrintable(id)
    val atLeastAdds = feature_add != null && feature_add.length > 0
    val atleastRemoves = feature_remove != null && feature_remove.length > 0

    goodId && (atLeastAdds || atleastRemoves) && !feed.blackCookies.contains(id)
  }

  override def parse(implicit uid: UniqueID) = { record =>
    val (id, feature_add, feature_remove) = record
    logger.debug("Data record => %s".format(record))

    case class localSegment(network:String, localId:String)

    val add_features = {
      if (feature_add != null && feature_add.size > 0) feature_add.split(',').filter(seg => seg.length > 0 && seg.contains('.')).toList map { seg =>
        val Array(network,id) = seg.split('.')
        localSegment(network,id)
      }
      else List[localSegment]()
    }
    val remove_features = {
      if (feature_remove != null && feature_remove.size > 0) feature_remove.split(',').filter(seg => seg.length > 0 && seg.contains('.')).toList map { seg =>
        val Array(network,id) = seg.split('.')
        localSegment(network,id)
      }
      else List[localSegment]()
    }
    val timestampMillis = System.currentTimeMillis
    val profileEvents = ListBuffer[InputEvent]()

    def fill(grouping:List[localSegment], operation:String) {
      val groups = grouping.groupBy(_.network)
      for((net,segs) <- groups) {
        val lc:Map[CharSequence,CharSequence] = Map()
        val event = preBuild(id, DataPipe.EdgeId, timestampMillis)
        lc += (OperationType.name -> operation)
        lc += (Network.name -> net)
        lc += (Segments.name -> segs.map(seg => (seg.network + "." + seg.localId) ).mkString(",") )
        event.setChange(lc)
        profileEvents += event
      }
    }

    if(add_features.nonEmpty) fill(add_features, Add)
    if(remove_features.nonEmpty) fill(remove_features, Remove)
    profileEvents.toList
  }
}
