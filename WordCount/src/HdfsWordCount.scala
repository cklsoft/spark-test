import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object HdfsWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0)/*"yarn-standalone"*/,"myWordCount",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    							//List("lib/spark-assembly_2.10-0.9.0-incubating-hadoop1.0.4.jar")
    val logFile = sc.textFile(args(1))//"hdfs://master:9101/user/root/spam.data") // Should be some file on your system
  //  val file = sc.textFile("D:\\test.txt")
    val counts = logFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    println(counts)
    counts.saveAsTextFile(args(2)/*"hdfs://master:9101/user/root/out"*/)
  }
}