package org.matruss.waterfall

import com.twitter.scalding._
import org.apache.log4j.Level
import Definitions._
import collection.mutable.{Map => MutableMap}

class Runnable(args:Args) extends Job(args) with Logging {

  if(args.boolean("debug")) setLogLevel(Level.DEBUG)

  private[this] val feeds = MutableMap.empty[Symbol,Feed]
  private[this] val coutput = args.getOrElse(CommonOutput.name,"")

  args.m foreach { case(k,v) =>
    logger.debug("Arguments => key %s, value %s".format(k,v))
  }

  def isProductionMode:Boolean = {
    if(args.boolean("testmode")) false
    else true
  }

  val blackCookies = Set( args.getOrElse("black-cookies-list", "").split(",").map(_.trim) : _*)

  FeedTypes foreach { cat  =>
    val (in,out) = ( args.getOrElse(cat.title.name,""), args.getOrElse(cat.outPath.name,"") )
    val fd = cat.copy(in, out, blackCookies)
    isProductionMode match {
      case true => {
        if (fd.isRequired && !(fd.validate) ) {
          args.m foreach { case(k,v) =>
            logger.error("Arguments => key %s, value %s".format(k,v))
          }
          logger.error("No mandatory " + fd.title.name + " log specified")
          throw new RuntimeException("No mandatory " + fd.title.name + " log specified")
        }
        else if (fd.validate) feeds += (fd.title -> fd)
      }
      case false => if (fd.validate) feeds += (fd.title -> fd)
    }
  }

  if( !(coutput.length > 0 || feeds.values.forall(_.output.length > 0) ) )
    throw new RuntimeException("Must specify either common output or output for each individual feed")

  // if no output is set, use generic one
  if( (coutput.length > 0) )
    feeds filterNot { case(k,v) => v.output.length > 0 } foreach { case(k,v) => feeds(k) = v.copy(output = coutput) }

  feeds map { case(id, feed) => PipeDefs.Pipes.get(id).get(feed,mode,flowDef) } filter {_.hasData} foreach { _.run }

  override  def config : Map[AnyRef,AnyRef]  = {
    super.config ++
    // resolves "ClassNotFoundException newcascading.*" exception on a cluster (hopefully)
    Map("cascading.app.appjar.class" -> classOf[Runnable]) ++
    // Additional HDFS config
    (mode match {
      case Hdfs(conf, _) => Map(
          "mapreduce.framework.name"         -> "yarn",
          "mapred.job.tracker"               -> "yarn"
        ) ++ Map(
          "mapreduce.job.credentials.binary" -> Option(System.getenv("HADOOP_TOKEN_FILE_LOCATION"))
        ).collect { case (k, Some(v)) => k -> v }
      case _=> Map.empty[String, String]
    })
  }
}

