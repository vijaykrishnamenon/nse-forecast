package cb.amrita.cen.stockmarket

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object NseDayTag {

  def main(args: Array[String]) = {
    
    val log = LoggerFactory.getLogger("NSEDayTager")
    val conf = new SparkConf().setMaster("spark://cen-server:7077").setAppName("NSEDayTag")    
    val sc = new SparkContext(conf)
       
    val files = sc.textFile("hdfs://cen-server:5055/user/censpark/listfile").map { l =>
      val nsei = l.lastIndexOf("NSE") + 3
      l.substring(nsei)
    }.collect
        
    for (i <- 0 until files.length) {
       
      val md = files(i).substring(files(i).lastIndexOf('/')+1, files(i).indexOf('.'))     
      
      val fullRDD = sc.textFile("hdfs://cen-server:5055/user/censpark/NSE/NSE"+files(i),32).map(l => md+'|'+l).cache
      
      fullRDD.saveAsTextFile("hdfs://cen-server:5055/user/censpark/newNse/"+files(i)) 
      fullRDD.unpersist(false)
    }   
  }    
}