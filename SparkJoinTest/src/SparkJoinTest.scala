import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object SparkJoinTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0)/*"yarn-standalone"*/,"SparkJoinTest",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
                                                        //List("lib/spark-assembly_2.10-0.9.0-incubating-hadoop1.0.4.jar")
    val txtFile = sc.textFile(args(1))//"hdfs://master:9101/user/root/spam.data") // Should be some file on your system
    val rating=txtFile.map(line =>{
    	val fileds=line.split("::")
    	(fileds(1).toInt,fileds(2).toDouble)
    	}
    )//大括号内以最后一个表达式为值
    val movieScores=rating.groupByKey().map(
        data=>{
          val avg=data._2.sum/data._2.size
       //   if (avg>4.0) 
            (data._1,avg)
        }
    )
    
    val moviesFile=sc.textFile(args(2))
    val moviesKey=moviesFile.map(line =>{
      val fileds=line.split("::")
      (fileds(0).toInt,fileds(1))
      }
    ).keyBy(tuple=>tuple._1)//设置健
    
    val res=movieScores.keyBy(tuple=>tuple._1).join(moviesKey)// (<k,v>,<k,w>=><k,<v,w>>)
    .filter(f=>f._2._1._2>4.0)
    .map(f=>(f._1,f._2._1._2,f._2._2._2))
    res.saveAsTextFile(args(3))
  }
}
