package cb.amrita.cen.stockmarket

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Nifty50Minutes {

    def main(args: Array[String]) = {
    val log = LoggerFactory.getLogger("MinTag")
    val conf = new SparkConf().setMaster("spark://cen-server:7077").setAppName("MinTag").set("spark.cores.max", "16")
    val sc = new SparkContext(conf)

    val files = List("Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun") 
    
    val nifty50 = sc.textFile("hdfs://cen-server:5055/user/censpark/Nifty50.txt").collect      
    
    for (i <- 0 until files.length) {

      val trades50RDD = sc.textFile("hdfs://cen-server:5055/user/censpark/newNse/"+files(i)+"/*/*", 64).map { l =>
        val str = l.split('|')
        val (md, id, stk, ts, prc, vol) = (str(0), str(1), str(2), str(4).substring(0,5), str(5).toFloat, str(6).toFloat)
        ((stk, ts, md), (id, prc, vol))
      }.filter(nifty50 contains _._1._1).reduceByKey { (x, y) =>
        val (xid, xprc, xvol) = x
        val (yid, yprc, yvol) = y
        val p = (xprc + yprc) / 2         
        val vol = xvol + yvol
        (yid, p, vol)
      }         
      
      val combRDD = trades50RDD.map(l => l._2._1 + '|' +
    		  							 l._1._1 + '|' +	
    		  							 l._1._3 + '|' + "|EQ|" +
    		  							 l._1._2 + '|' +
    		  							 l._2._2 + '|' +
    		  							 l._2._3 )       
      
      combRDD.saveAsTextFile("hdfs://cen-server:5055/user/censpark/minNse/"+files(i))    
    }

  }  
  
}