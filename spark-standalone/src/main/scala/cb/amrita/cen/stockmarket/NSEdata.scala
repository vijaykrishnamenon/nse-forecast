package cb.amrita.cen.stockmarket

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf 
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object NSEdata {

  def main(args: Array[String]) = {
    val log = LoggerFactory.getLogger("NSEData");
    
    val conf = new SparkConf().setMaster("spark://cen-server:7077").setAppName("NseData").set("spark.cores.max", "16")
    val sc = new SparkContext(conf)

    val cmpy = sc.textFile("hdfs://cen-server:5055/user/censpark/companyList").collect

    val files = sc.textFile("hdfs://cen-server:5055/user/censpark/fileList").map { l =>
      val nsei = l.lastIndexOf("NSE") + 3
      l.substring(nsei)
    }.collect

    for (i <- 0 until files.length) {
      val cdata = sc.textFile("hdfs://cen-server:5055/user/censpark/resultNse" + files(i) + "/*").map { l =>
        val cName = l.substring(1, l.indexOf(','))
        val listStr = l.substring(l.indexOf('(', 2) + 1, l.lastIndexOf(')') - 1)
        (cName, listStr)
      }
      for (j <- 0 until cmpy.length) {
        val fcdata = cdata.filter { c => if (c._1.compareTo(cmpy(j)) == 0) true else false }
        	.flatMap(c => c._2.split(','))
        fcdata.saveAsTextFile("hdfs://cen-server:5055/user/censpark/orgNse" + files(i) + "/" + cmpy(j))
      }
    }
  }
}