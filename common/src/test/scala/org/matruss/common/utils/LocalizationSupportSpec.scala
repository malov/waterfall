package org.matruss.common.utils

import java.io.File

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LocalizationSupportSpec extends Specification with Mockito {

  "A LocalizationSupport object" should {
    "copy zkdump to map reduce dir" in {

      val LocalFileName = "/somePath/someFile"
      val LinkName = "someLink"
      val TempFileName = "/someTempFilePath/someName"

      val mockFs = mock[FileSystem]

      val obj = new Configured() with LocalizationSupport {
        override def getFileSystem(): FileSystem = mockFs
        override def getTempFileName(linkName: String): String = TempFileName
      }

      val file = new File(LocalFileName)
      val result = obj.makeLocalizableUri(file, LinkName)

      there was one(mockFs).copyFromLocalFile(new Path(LocalFileName), new Path(TempFileName))
      there was one(mockFs).deleteOnExit(new Path(TempFileName))

      result.getPath must beEqualTo(TempFileName)
      result.getFragment must beEqualTo(LinkName)
    }
  }

}