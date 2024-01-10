package ua.nlp.ukrlm
package models

case class HashBand(bytes: Array[Byte]) {
  override def equals(obj: Any): Boolean = {
    obj match {
      case HashBand(otherBytes) => java.util.Arrays.equals(bytes, otherBytes)
      case _ => false
    }
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(bytes)
}
