package ua.com.lifecell

import io.circe.{Decoder, parser}


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
