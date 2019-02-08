package hdfsUtils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.FileSystem

class StringifyLog(appName:String, fs:FileSystem) extends hdfsWriteFile(appName:String, fs:FileSystem) {
  private def now:String ={
    val format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    format.format(Calendar.getInstance().getTime())
  }

  private def log(status:String, message:String):String = s"$now [$appName] $status - $message"

  def info(message: Any): Unit = super.write(log( "INFO ", message.toString))
  def warn(message: Any): Unit = super.write(log( "WARN ", message.toString))
  def error(message: Any): Unit = super.write(log("ERROR", message.toString))
}
