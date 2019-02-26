package loadtohive

import java.text.SimpleDateFormat
import java.util.Calendar

import hdfsUtils.StringifyLog
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.xml.Node


object StructureGenerator {
  def createStructType(a:List[Node], b:List[Node]):StructType ={
    val result = ArrayBuffer[StructField]()
    val concat = List(a , b).transpose
    concat.foreach(x=> {
      val n:StructField = StructField(x(0).toString,StringType)
      result += n
    })
    StructType(result)
  }
}


object collectToParquet {

  def measure[T](operation: => T): Long = {
    val time = System.currentTimeMillis()
    operation
    System.currentTimeMillis() - time
  }

  def worker(sc: SparkContext, sqlc:SQLContext, schema:StructType , typeSchema:Map[String, String], listOfPaths:List[String], outputPath:String, numPart:Int, fileCoalesce:Int):Unit = {
    val dirs = listOfPaths.mkString(",")
    val lines = sc.textFile(dirs)//.repartition(numPart)//.coalesce(fileCoalesce
    val rowRDD = lines.map(x => x.split("\\|", -1)).map(x => {Row.fromSeq(x)})
    val tempDF = sqlc.createDataFrame(rowRDD, schema)
    tempDF
      .columns
      .foldLeft(tempDF) {
       (table, colName) => typeSchema(colName).toString match {
          case "string" => table.withColumn(colName, col(colName))
          case "int"    => table.withColumn(colName, col(colName).cast("int"))
          case "bigint" => table.withColumn(colName, col(colName).cast("bigint"))
          case "float"  => table.withColumn(colName, col(colName).cast("float"))
          case _        =>  throw new NoSuchElementException
        }
      }
      .repartition(numPart).write.mode(SaveMode.Append).parquet(outputPath)
  }

  def hadoopListFolders(directoryName: String, fs:FileSystem, buff:Int):List[String] = {
    var folderInfo = scala.collection.mutable.Map[Path, Int]()
    val folders = fs.listStatus(new Path(directoryName))
    folders.foreach( f => {
      lazy val lastFolder = folders.map(_.getPath).last
      val rollingPath = f.getPath
      if (fs.getContentSummary(rollingPath).getFileCount > 0  && lastFolder != rollingPath && !fs.exists(new Path(rollingPath + "/_temporary"))){
        folderInfo += rollingPath -> (fs.getContentSummary(rollingPath).getLength / 1000000).toInt
      }
    })
    val res:ArrayBuffer[String] = ArrayBuffer.empty[String]
    folderInfo.foldLeft(0)((accum, element) => {
      if (accum <= buff){res += element._1.toString + "/part*"}
      accum + element._2
    })
    res.sorted.toList
  }

  def createPartitionDir(tableName:String, fs:FileSystem, sch:String, numPart:Int):String ={
    val mainPath = "hdfs:///user/hive/warehouse/"+ sch + ".db/"  //
    val folderPattern = "yyyyMMddHH"
    val timeFormat = new SimpleDateFormat(folderPattern)
    val now = timeFormat.format(Calendar.getInstance.getTime)
    val result = mainPath + tableName.toLowerCase + "/dt=" +  now  //Linux
    fs.mkdirs(new Path(result))
    result
  }

  def deleteFile(filename: ListBuffer[String], fs:FileSystem):Unit = {
    filename.par.foreach(path => {
      fs.delete(new Path(path.split("/part*")(0)),true)
    })
  }

  def main(args: Array[String]): Unit = {
    //Init spark context
    val appName = args(1).toString
    val conf = new SparkConf().setAppName(appName).set("spark.speculation","false")
    val sc = new SparkContext(conf)
    val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec.", "snappy")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val log = new StringifyLog(appName, fs)

    val tempPatch = "/tmp/" + args(0).toString.split("/").last
    fs.copyToLocalFile(false, new Path(args(0).toString), new Path(tempPatch), true)
    val configFile = scala.xml.XML.loadFile(tempPatch)
    //Parse XML-config file
    val statesTable = (configFile \\ "@tablename").toList
    var statesName = scala.collection.mutable.Map[String, List[Node]]()
    var statesType = scala.collection.mutable.Map[String, List[Node]]()
    var statesMain = scala.collection.mutable.Map[String, Map[String, String]]()
    var dbSchema = scala.collection.mutable.Map[String, String]()
    var numPartitions = scala.collection.mutable.Map[String, Int]()
    var fileCoalesce = scala.collection.mutable.Map[String, Int]()
    var fileBufferSizeInMB = scala.collection.mutable.Map[String, Int]()

    for {
      item <- configFile \\ "table"
    } yield {
      statesName += ((item \\ "@tablename").toString -> (item \\ "@name").toList)
      statesType += ((item \\ "@tablename").toString -> (item \\ "@type").toList)
      val nameList = (item \\ "@name").toList.map(_.toString)
      val typeList = (item \\ "@type").toList.map(_.toString)
      dbSchema += ((item \\ "@tablename").toString -> (item \\ "@schema").toString())
      numPartitions += ((item \\ "@tablename").toString -> (item \\ "@numPartitions").toString.toInt)
      fileCoalesce += ((item \\ "@tablename").toString -> (item \\ "@coalesce").toString.toInt)
      fileBufferSizeInMB += ((item \\ "@tablename").toString -> (item \\ "@fileBufferSizeInMB").toString.toInt)
      statesMain += ((item \\ "@tablename").toString -> (nameList zip typeList).toMap)
    }

    val statesFilePatchList = (configFile \\ "@path").toList
    val statesFilePatch = ( statesTable zip statesFilePatchList).toMap
    val deleteBuffer = scala.collection.mutable.ListBuffer.empty[String]

    val serialTime = measure {
      statesTable.par.foreach(item => {
        try {
          val strItem = item.toString
          val schema = StructureGenerator.createStructType(statesName(strItem), statesType(strItem))
          val typeSchema = statesMain(strItem)
          val hivePartition = createPartitionDir(strItem, fs, dbSchema(strItem), numPartitions(strItem))
          val hdfsDirs = hadoopListFolders(statesFilePatch(item).toString, fs, fileBufferSizeInMB(strItem))
          worker(sc, sqlc, schema, typeSchema, hdfsDirs, hivePartition, numPartitions(strItem), fileCoalesce(strItem))
          sqlc.sql("MSCK REPAIR TABLE " + dbSchema(strItem) + "." + strItem)
          hdfsDirs.map(deleteBuffer += _)
        }
        catch {
            case e: Exception =>
              log.error(e)
        }
      })
    }
    deleteFile(deleteBuffer, fs)
  }
}
