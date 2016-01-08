package org.matruss.waterfall

import com.twitter.scalding.{Args, Job}
import com.twitter.scalding.{Tool => ScaldingTool}
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j.Level
import org.apache.hadoop.fs.{FileSystem, Path}
import collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configured

object Niagara extends App {
  val job = { jobargs:Args =>

    new CascadeStatJob(jobargs, List(new Runnable(_)), List(("waterfall","rejected-events")))
  }
  val runnable = new ScaldingTool; runnable.setJobConstructor(job)

  val status = ToolRunner.run(runnable, args)
  if(status != 0) { throw new RuntimeException("ToolRunner returned nonzero exit status %d" format status) }
}

object Definitions {

  val FeedTypes:List[Feed] = List(new DataFeed)

  val CommonOutput = 'output

  val OutputRoot = "/part-m-"
}
