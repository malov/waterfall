package org.matruss.common.utils

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

/**
 * This trait provides support for Configurable instances that need access to
 * auxiliary (or side) data, where that data is too big to store on the job
 * configuration and must be stored in the file system.
 */
trait LocalizationSupport extends Configurable {

  private val logger = Logger.getLogger(getClass)

  def debug(msg: String) = logger.debug(msg)

  /**
   * Given a local file and a target link name, do all the processing required
   * to create a localizable URI object.  The caller can use the resulting URI
   * in a call to job.addCacheFile(URI) in order to make the file available to
   * the job.  The job can then access the file using linkName.
   *
   * The processing steps are:
   * - generate a unique temp file name (using system time in millis)
   * - copy the local file to the temp location on the appropriate remote FS
   * - mark the newly created temp file to be deleted when the JVM exits
   * - return the URI of the temp file path, with an anchor indicating the
   *   desired link name.  In URI teminology, this anchor is referred to
   *   as a "fragment".
   */
  def makeLocalizableUri(localFile: File, linkName: String): URI = {
    debug("localFile=%s, linkName=%s".format(localFile, linkName))

    val tempName = getTempFileName(linkName)
    val tempPath = new Path(tempName)

    val remoteFs = getFileSystem
    remoteFs.copyFromLocalFile(new Path(localFile.getPath), tempPath)
    remoteFs.deleteOnExit(tempPath)
    val result = new URI(tempName + (if(linkName != null) "#" + linkName else ""))

    debug("copy=%s, remoteFs='%s', result=%s".format(tempPath, remoteFs, result))
    result
  }

  def makeLocalizableUri(localFile: File): URI = makeLocalizableUri(localFile, null)

  /*
   * Provided for testing.
   */
  private[utils] def getTempFileName(linkName: String): String = "/tmp/%s.%d".format(linkName, System.currentTimeMillis)
  private[utils] def getFileSystem(): FileSystem = FileSystem.get(getConf)
}
