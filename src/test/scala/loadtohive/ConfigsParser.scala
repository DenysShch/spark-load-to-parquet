package loadtohive

import io.circe.parser
import io.circe._
import io.circe.generic.auto._
import scala.collection.immutable.ListMap

case class Xdr(name: String, path: String, sink:String, schema:String, partitions:Int,
               fileBuffer:Int, partitionField: String, data: List[Field]) {

  def dataToMap:ListMap[String, String] = ListMap(
    data.map(x => x.fieldName → x.fieldType) ++ List("dt" → "string") : _*
  )

  def getIndex:Int = data.indexOf(Field(partitionField, "string"))

}

case class Field(fieldName:String, fieldType: String)

object ConfigsParser {

  implicit val decodeFields: Decoder[Field] =
    Decoder.forProduct2("name", "type")(Field.apply)

  implicit val decodeFieldType: Decoder[Xdr] =
    Decoder.forProduct8("name", "path", "sink", "schema", "partitions",
      "fileBuffer", "partitionField", "data")(Xdr.apply)


  def readConfig(path:String):Xdr = {
    val source = scala.io.Source.fromFile(path)
    val input = try source.mkString finally source.close()
    val decodingResult = parser.decode[Xdr](input)
    decodingResult match {
      case Right(res) => res
      case Left(error) => {
        println(error)
        throw new Exception(s"Can't read config file from path $path.")
      }
    }

  }




}
