package edu.usc.irds.sparkler.util


import org.apache.hadoop.fs.{FileSystem, Path}
import edu.usc.irds.sparkler.model
import edu.usc.irds.sparkler.model.FetchedData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.nutch.metadata.Metadata
import org.apache.nutch.protocol.Content
import org.apache.nutch.util.NutchConfiguration

import scala.collection.JavaConversions._



/**
  * Provides utilities for bridging Nutch and Sparkler
  */
object NutchBridge {

  /**
    * Converts Sparkler's content to Nutch Content model.
    * @param rec Sparkler's content
    * @param conf Hadoop configuration (required by Nutch)
    * @return Nutch's Content
    */
  def toNutchContent(rec: FetchedData, conf: Configuration): Content = {
    rec.getContent
    val cnt = new Content(rec.getResource.getUrl, rec.getResource.getUrl, rec.getContent,
      rec.getContentType, toNutchMetadata(rec.getMetadata), conf)
    cnt
    cnt
  }

  def toNutchMetadata(meta: model.MultiMap[String, String] ): Metadata  ={
    val mutchMeta  = new Metadata
    for (key <- meta.keySet()){
      for (value <- meta.get(key)) {
        mutchMeta.add(key, value)
      }
    }
    mutchMeta
  }

  def fromNutchContent(path: String): Unit = {
    val conf = NutchConfiguration.create()
    val fs = FileSystem.get(conf)
    val file = new Path(path)
    val reader = new SequenceFile.Reader(fs, file, conf)
    val webdata = Stream.continually{
      val key = new Text()
      val value = new Content()
      reader.next(key, value)
      (key, value)
    }
    val iter = webdata.toIterator
    var i = 0
    while (i < 5) {
      val cur = iter.next()
      println(cur)
      i+=1
    }
  }
}
