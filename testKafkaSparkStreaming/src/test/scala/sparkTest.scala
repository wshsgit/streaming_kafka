import org.apache.spark.{SparkConf, SparkContext}

object sparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountext")
    val sc = new SparkContext(conf)
    sc.textFile("/input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("/output")
  }
}
