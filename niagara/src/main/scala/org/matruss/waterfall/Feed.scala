package org.matruss.waterfall

import collection.mutable.Buffer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.{Configuration, Configured}

abstract class Feed(val input:String, val output:String, val blackCookies:Set[String]) extends Configured with Logging {

  def title:Symbol
  def outPath:Symbol

  def copy(input:String = this.input, output:String = this.output, blackCookies:Set[String] = this.blackCookies):Feed

  def validate:Boolean = if (input != null && input.length > 0) true else false

  def isRequired:Boolean = true

  def expand(conf:Configuration):Buffer[String] = {
    val value = Buffer[String]()
    if ( input.length > 0 ) {
      input.split(",").foreach { loc =>
        val pattern = new Path(loc)
        val glob = FileSystem.get(conf).globStatus(pattern)
        if(glob.length > 0) { glob foreach { path => value += path.getPath.toUri.getPath } }
      }
    }
    value
  }
}

class DataFeed(override val input:String = "", override val output:String = "", override val blackCookies: Set[String] = Set[String]()) extends Feed(input,output,blackCookies) {

  val title = 'data
  val outPath = 'data

  override def isRequired = false

  def copy(input:String = this.input, output:String = this.output, blackCookies:Set[String] = this.blackCookies):DataFeed = new DataFeed(input, output, blackCookies)
}
