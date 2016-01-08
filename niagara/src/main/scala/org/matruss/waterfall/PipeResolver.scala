package org.matruss.waterfall

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.pipe.Pipe
import cascading.tap.hadoop.io.MultiInputSplit
import com.twitter.scalding._
import cascading.tuple.Fields
import cascading.flow.FlowDef
import org.apache.hadoop.mapred.FileSplit
import collection.mutable.Map
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.Stat


case class CustomDelimitedMultipleFiles (p : Seq[String],
                                         override val separator:String,
                                         override val fields : Fields = Fields.ALL,
                                         override val skipHeader : Boolean = false,
                                         override val writeHeader: Boolean = false) extends FixedPathSource(p:_*) with DelimitedScheme {
  override val strict = false
}

case class Cdf (p : String,
                override val separator:String,
                override val fields : Fields = Fields.ALL,
                override val skipHeader : Boolean = false,
                override val writeHeader: Boolean = false) extends FixedPathSource(p) with DelimitedScheme

abstract class GenericPipe(val feed:Feed) {

  type InputSchema

  implicit val mode:Mode
  implicit val flowDef:FlowDef

  protected val re="""^.*\[\d+\]:(.*)$""".r
  protected val changes:Map[CharSequence,CharSequence] = Map()

  def schema:List[Symbol]
  def projection:List[Symbol]
  def hasData:Boolean = {
    mode match {
      case Hdfs(_,configuration) => feed.expand(configuration).size > 0
      case _ => true
    }
  }


  implicit def symbolToFields(fs:List[Symbol]):Fields = {
    new Fields(fs.map(_.name).toSeq : _*)
  }
  protected val separator = "\t"
  protected def getPipe:Pipe = {
    mode match {
      case Hdfs(_, configuration) => CustomDelimitedMultipleFiles(feed.expand(configuration), separator, fields = schema).read
      case _ => Cdf(feed.input, separator, fields = schema).read
    }
  }
  def run(implicit uid: UniqueID): TypedPipe[InputEvent]
  def screen(implicit uid: UniqueID):InputSchema => Boolean
  def parse(implicit uid: UniqueID):InputSchema => List[InputEvent]

}


trait DecoratetedGenericPipe extends GenericPipe{
  self: Logging =>
  trait AbstractDefaultObject[A]{
    def defVal : A
  }

  implicit object BooleanDefaultValue extends AbstractDefaultObject[Boolean] { def defVal = false }
  implicit object ListDefaultValue extends AbstractDefaultObject[List[InputEvent]] { def defVal = List[InputEvent]() }

  def decorateErrors[A : AbstractDefaultObject](parentParse:InputSchema => A)(record: InputSchema)(implicit uid: UniqueID) : A = {
    try{
      parentParse(record)
    }catch{
      case e: Exception => {
        val filename = {
          val hfp = RuntimeStats.getFlowProcessForUniqueId(uid).asInstanceOf[HadoopFlowProcess]
          val mis =  hfp.getReporter().getInputSplit.asInstanceOf[MultiInputSplit]
          mis.getWrappedInputSplit.asInstanceOf[FileSplit].getPath.toString
        }
        self.logger.error(s"Error in data, feed: ${feed.title}, row: ${record}, filename: ${filename}")
        Stat("rejected-events","gc-engine.waterfall").incBy(1)
        implicitly[AbstractDefaultObject[A]].defVal
      }
    }
  }

  abstract override def screen(implicit uid: UniqueID):InputSchema => Boolean = decorateErrors(super.screen)
  abstract override def parse(implicit uid: UniqueID):InputSchema => List[InputEvent] = decorateErrors(super.parse)

}

object PipeDefs {

  lazy val Pipes = Map[Symbol, (Feed,Mode,FlowDef) => GenericPipe] (
    'data -> { (feed,mode,flow) => new DataPipe(feed,mode,flow) with DecoratetedGenericPipe },
  )
}

trait HitEventType {

  private val Hit = "Hit"

  protected val Daystamp = 'daystamp
  protected val Context = 'context
  protected val HitType = 'hit_type

  protected def preBuild (id:String,edge:String,time:Long):InputEvent = {
    val event = new InputEvent
    event.setId(id)
    event.setOrigin(edge)
    event.setTstamp(time)
    event.setChangeType(Hit)
    event
  }
}

trait CommandEventType {

  private val Command = "Command"

  protected val Add = "ADD"
  protected val Remove = "REMOVE"
  protected val Replace = "REPLACE"

  protected val OperationType = 'operation_type
  protected val Network = 'network
  protected val Segments = 'segments
  protected val AssociationId = 'association_id

  protected def preBuild (id:String,edge:String,time:Long):InputEvent = {
    val event = new InputEvent
    event.setId(id)
    event.setOrigin(edge)
    event.setTstamp(time)
    event.setChangeType(Command)
    event
  }
}


