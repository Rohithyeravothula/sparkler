package edu.usc.irds.sparkler.pipeline

import java.math.BigInteger
import java.security.MessageDigest

import edu.usc.irds.sparkler.base.CliTool
import edu.usc.irds.sparkler.{Constants, SparklerConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.nutch.protocol
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option
import org.apache.hadoop.fs.{FileSystem, Path}


object ReaderSeg extends CliTool with Serializable {


  // Load Sparkler Configuration
  val sparklerConf: SparklerConfiguration = Constants.defaults.newDefaultConfig()

  @Option(name = "-m", aliases = Array("--master"),
    usage = "Spark Master URI. Ignore this if job is started by spark-submit")
  var sparkMaster: String = sparklerConf.get(Constants.key.SPARK_MASTER).asInstanceOf[String]

  @Option(name = "-o", aliases = Array("--out"),
    usage = "Output path, default is job id")
  var outputPath: String = ""

  @Option(name = "-id", aliases = Array("--id"), required = true,
    usage = "Job id. When not sure, get the job id from injector command")
  var jobId: String = "/home/rohith/Projects/Personal/sparkler/j2/"

  val conf: SparkConf = new SparkConf().setMaster(sparkMaster).setAppName(jobId)

  val sc: SparkContext = new SparkContext(conf)

  def getHashcode(url: String, style: String = "md5"): String  = {
    // scalastyle:off magic.number
    new BigInteger(1, MessageDigest.getInstance(style).digest(url.getBytes)).toString(16)
  }

  def constructPath(code: String, prefix: String): String = {
    s"$prefix/${code.take(6).grouped(2).mkString("/")}/${code.takeRight(6)}"
  }

  def readContent(path: String): RDD[(String, String)]={
      val data = sc.sequenceFile[Text, protocol.Content](path)
      data.map(x => ( x._2.getUrl, new String(x._2.getContent)))
  }

  def createDump(path: String, output: String = outputPath): Unit = {
    val simple = sc.parallelize(1 to 10).map(x => (x.toString, ('A'.toInt + x).toChar.toString))

    val output = "/tmp/sparkler"
    val hdfsconf = sc.hadoopConfiguration


    simple.mapPartitions{ data =>
      val fs = FileSystem.get(hdfsconf)
      while(data.hasNext){
        val pp = data.next()
        val path = new Path(s"$output/${pp._1}")
        val os = fs.create(path)
        os.writeBytes(pp._2)
        os.close()
      }
      data
    }.collect()
  }

  def main(args: Array[String]): Unit = {
    createDump(args(1), outputPath)
  }

  override def run(): Unit = ???
}

object Tester extends App {
  ReaderSeg.createDump("", "")
//  val readerseg = new ReaderSeg()
//  readerseg.createDump("", "")

}


