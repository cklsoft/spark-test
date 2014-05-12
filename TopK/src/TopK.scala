import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object TopK {
  def main(args: Array[String]) {
    //yarn-standalone hdfs://master:9101/user/root/spam.data 5
    val sc = new SparkContext(args(0)/*"yarn-standalone"*/,"TopK",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    val logFile = sc.textFile(args(1))//"hdfs://master:9101/user/root/spam.data") // Should be some file on your system
    val counts = logFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    val sorted=counts.map{
      case(key,val0) => (val0,key)
    }.sortByKey(true,1)
    val topK=sorted.top(args(2).toInt)
    topK.foreach(println)
  }
}
