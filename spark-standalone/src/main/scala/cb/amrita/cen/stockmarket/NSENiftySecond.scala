package cb.amrita.cen.stockmarket

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object NSEMuthuSecond {
  def main(args: Array[String]) = {
    val log = LoggerFactory.getLogger("NiftyTag")
    val conf = new SparkConf().setMaster("spark://cen-server:7077").setAppName("NiftyTag").set("spark.cores.max","16")
    val sc = new SparkContext(conf)

    val files = sc.textFile("hdfs://cen-server:5055/user/censpark/listfile").map { l =>
      val nsei = l.lastIndexOf("NSE") + 3
      l.substring(nsei)
    }.collect
    
    //val nifty50 = sc.textFile("hdfs://cen-server:5055/user/censpark/Nifty50.txt").collect
    
    val nifty172 =  sc.textFile("hdfs://cen-server:5055/user/censpark/ncomm500.txt").map(l=> (l,1)).
    		reduceByKey(_+_).map(p=>p._1).
    		map(_.trim()).
    		filter(!_.isEmpty).
    		filter(_.indexOf(' ')<0).
    		filter(_.matches("[^0-9]*")).collect
    
    for (i <- 0 until files.length) {

      val md = files(i).substring(files(i).lastIndexOf('/')+1, files(i).indexOf('.')) 

      val tradesRDD = sc.textFile("hdfs://cen-server:5055/user/censpark/NSE/NSE" + files(i), 32).map { l =>
        val str = l.split('|')
        val (id, stk, ts, prc, vol) = (str(0), str(1), str(3), str(4).toFloat, str(5).toFloat)
        ((stk, ts), (id, prc, vol, md))
      }.reduceByKey { (x, y) =>
        val (xid, xprc, xvol, xmd) = x
        val (yid, yprc, yvol, ymd) = y
        val (id, p) = if (xid > yid) (xid, xprc) else (yid, yprc)
        val vol = xvol + yvol
        (id, p, vol, xmd)
      }
      
      val niftyRDD = sc.textFile("hdfs://cen-server:5055/user/censpark/Nifty/"+md+".nft").map { l =>
      val str = l.split('|')
      ( str(1), str(2).toFloat )
      }.reduceByKey((x,y)=>(x+y)/2)
      
      val trades172RDD = tradesRDD.filter(nifty172 contains _._1._1)
      val combRDD = trades172RDD.map(l => (l._1._2 , (l._2._1 , l._1._1 , l._2._2 , l._2._3, l._2._4)) ).join(niftyRDD)
      val cmappedRDD =  combRDD.map(l => l._2._1._1 + '|' + l._2._1._2 +"|EQ|" +
    		  			l._2._1._5 + '|' +l._1 +'|' + l._2._1._3 +'|'+ l._2._1._4 +'|'+ l._2._2)
      
      cmappedRDD.saveAsTextFile("hdfs://cen-server:5055/user/censpark/muthuNse/"+md)    
    }

  }

}