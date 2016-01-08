package org.matruss.waterfall.util

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}

object Conversions {

  val formats = Array("yyyy-MM-dd HH:mm:ss 'UTC'",
                      "yyyy-MM-dd HH:mm:ssZZ")
  lazy val LogTimeFormat = new DateTimeFormatterBuilder().append( null, formats.map( DateTimeFormat.forPattern(_).getParser) ).toFormatter()

  val DaystampFormat = new DateTimeFormatterBuilder()
    .appendYear(4,4)
    .appendMonthOfYear(2)
    .appendDayOfMonth(2)
    .toFormatter

  object TimestampString {
    def apply(tstamp:Long) = new DateTime(tstamp,DateTimeZone.UTC).toString(LogTimeFormat)

    def unapply(s:String):Option[Long] = {
      try { Some(LogTimeFormat.withZone(DateTimeZone.UTC).parseDateTime(s).getMillis) }
      catch {
        case e: Exception => None
      }
    }
  }

  object Daystamp {
    def apply(dt:DateTime) = dt.withZone(DateTimeZone.UTC).toString(DaystampFormat)


    def unapply(s:String):Option[DateTime] = {
      try { Some(DaystampFormat.withZone(DateTimeZone.UTC).parseDateTime(s)) }
      catch {
        case e:Exception => None
      }
    }
  }
}
