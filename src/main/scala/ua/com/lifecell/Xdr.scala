package ua.com.lifecell

import scala.collection.immutable.ListMap

case class Xdr(name: String, path: String, sink:String, schema:String, partitions:Int,
               fileBuffer:Int, partitionField: String, data: List[Field]) {

  def dataToMap:ListMap[String, String] = ListMap(
    data.map(x => x.fieldName → x.fieldType) ++ List("dt" → "string") : _*
  )

  def getIndex:Int = {
    if (partitionField == "-1") -1
    else {
      val index = {
        List(
          data.indexOf(Field(partitionField, "string")),
          data.indexOf(Field(partitionField, "int")),
          data.indexOf(Field(partitionField, "bigint"))
        ).max
      }
      index match {
        case -1 => data.indexOf(Field(partitionField, "int"))
        case _  => index
      }
    }
  }


}

case class Field(fieldName:String, fieldType: String)