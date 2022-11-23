package ua.com.lifecell

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap
import scala.collection.parallel.{ParSeq, immutable}

object Main {

  def convertTypes(value:String, valueIndex: Int, schemaList:List[String]):Either[String, Any] = {
    schemaList(valueIndex) match {
      case "int"    ⇒
        try{
          Right(value.toInt)
        } catch {
          case _: Throwable=> Left(s"Can't convert value $value  with index [$valueIndex] to Int")
        }

      case "bigint" ⇒
        try {
          Right(value.toLong)
        } catch {
          case _: Throwable=> Left(s"Can't convert value $value  with index [$valueIndex] to Long")
        }
      case _        ⇒ Right(value)
    }
  }

  def createIndex(value:String):Array[String] = Array(
    Instant
      .ofEpochSecond(value.toInt)
      .atZone(ZoneId.of("Europe/Kiev"))
      .format(DateTimeFormatter
        .ofPattern("yyyyMMddHH")
      )
  )

  def dummyIndex:Array[String] = Array(
    LocalDateTime
      .now()
      .format(DateTimeFormatter
        .ofPattern("yyyyMMddHH")
      )
  )

  def readLocalFiles(sp:SparkSession, files: List[String], schemaMap:ListMap[String, DataType],
                     schemaList:List[String], outputPath:String, partitions:Int,
                     partitionIndex:Int): Unit = {

    //create schema
    val schema = StructType(schemaMap.map(x ⇒  StructField.apply(x._1, x._2, nullable = true)).toArray)

    //read file
    val rdd: RDD[Row] = sp.sparkContext
      .textFile(files.mkString(","))
      .map(x => x.split("\\|", -1)
        .zipWithIndex
        .map { case (element, index) =>
          if (element.nonEmpty) {
            convertTypes(element, index, schemaList) match {
              case Right(b) => b
              case Left(a) =>
                throw new NumberFormatException(a)
                sp.close()
            }
          } else null
        }
      )
      .filter(f => f.length > 1)
      .map(x => {
        val getIndex = partitionIndex match {
          case -1 ⇒ dummyIndex
          case _  ⇒ createIndex(x(partitionIndex).toString)
        }
        Row.fromSeq(x  ++ getIndex)
      })


    //apply the schema to the RDD.
    val df = sp.createDataFrame(rdd, schema)

    //write df
    df
      .coalesce(partitions)
      .write
      .partitionBy("dt")
      .mode(SaveMode.Append)
      .option("compression","gzip")
      .parquet(outputPath)
  }

  def convertToSparkType(strType:String):DataType = {
    strType match {
      case "string" ⇒ StringType
      case "int"    ⇒ IntegerType
      case "bigint" ⇒ LongType
    }
  }

  def hadoopListFiles(directoryName: Path, fs:FileSystem, buff:Int = 1000):List[String] = {

    def getTime(x: => Long):Long = x

    //remove corrupted files
    fs
      .listStatus(directoryName)
      .filter(f => f.isFile)
      .filter(f => f.getLen <= 1)
      .filter(f => getTime(System.currentTimeMillis()) - f.getModificationTime  > 1000000)
      .map(i => i.getPath.toString)
      .toList
      .foreach(file => fs.delete(new Path(file), true))

    fs
      .listStatus(directoryName)
      .filter(f => f.isFile)
      .filter(f => !f.getPath.getName.contains("_temporary"))
      .map(i => i.getPath.toString)
      .toList
      .sorted
      .take(buff)
  }

  def deleteFile(filename: List[String], fs:FileSystem):Unit = {
    if (filename.nonEmpty){
      filename.map(path => {
        fs.delete(new Path(path.split("/part*")(0)),true)
      })
    }
  }

  def readerWrapper(sp: SparkSession, files: List[String], schemaMap: ListMap[String, DataType],
                    schemaList: List[String], outputPath: String, partitions: Int,
                    partitionIndex: Int): Unit = {
    try {
      readLocalFiles(sp, files, schemaMap, schemaList, outputPath, partitions, partitionIndex)
    } catch {
      case e:Exception =>
        println(s"Error ${e.getLocalizedMessage}, try file checking...")
        val checkedFiles: immutable.ParSeq[Either[String, String]] = files.par.map(checker(sp, _))
        val goodFiles = checkedFiles.toList.filter(_.isRight).map(_.right.get)
        readLocalFiles(sp, goodFiles, schemaMap, schemaList, outputPath, partitions, partitionIndex)
    }
  }

  def checker(sp: SparkSession, file: String): Either[String, String] = {
      try {
        val trigger = sp.sparkContext
          .textFile(file)
          .map(x => x.split("\\|", -1))
          .count()
        println(s"File $file is ok -> $trigger")
        Right(file)
      } catch {
        case _: Exception =>
          println(s"File is corrupted $file")
          Left(file)
      }
  }

  def main(args: Array[String]): Unit = {
    val appConf = ConfigsParser.readConfig(args(0))

    val spark = SparkSession
      .builder()
      .appName(s"conv_to_par_${appConf.name}")
      .config("spark.hadoop.mapred.output.compress", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.hadoop.mapred.output.compression.type", "BLOCK")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
    spark.sqlContext.setConf("spark.sql.files.ignoreCorruptFiles", "true")
    spark.sqlContext.sql("set spark.sql.parquet.compression.codec=gzip")

    val listOfFiles = hadoopListFiles(new Path(appConf.path), fs, appConf.fileBuffer)

    if (listOfFiles.isEmpty) return println("Empty RDD !!!")

    val schema = appConf.dataToMap
    val convertedSchema: ListMap[String, DataType] = schema.map(x ⇒ (x._1, convertToSparkType(x._2)))
    val schemaList:List[String] = schema.valuesIterator.toList
    // partition index
    val partitionIndex: Int = appConf.getIndex

    readerWrapper(spark, listOfFiles, convertedSchema,
      schemaList, appConf.sink, appConf.partitions, partitionIndex)

    deleteFile(listOfFiles, fs)

    spark.sql("set role admin")
    spark.sql(s"MSCK REPAIR TABLE ${appConf.schema}.${appConf.name}")

  }

}
