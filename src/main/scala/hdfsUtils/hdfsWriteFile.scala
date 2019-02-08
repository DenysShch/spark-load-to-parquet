package hdfsUtils

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}

class hdfsWriteFile(appName:String, fs:FileSystem) {

  val hdfsPath = new Path(s"hdfs:///var/logs/$appName.log")

  private def fileExists(path: Path): Boolean = fs.exists(hdfsPath)

  private def fileCreate(s:String):Unit = {
    val file = fs.create(hdfsPath)
    try file.write((s + "\n").getBytes)
    finally file.close()
  }

  private def fileAppend(s:String):Unit = {
    try {
      val file = fs.append(hdfsPath)
      val writer = new PrintWriter(file)
      writer.append(s + "\n")
      writer.flush()
      file.hflush()
      writer.close()
      file.close()
    } catch {
      case e: Exception =>
        println(e)
    }
  }

  def write(s:String):Unit = {
    if (fileExists(hdfsPath)) fileAppend(s)
    else fileCreate(s)
  }

}
