package cb.amrita.cen.stockmarket 

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import akka.dispatch.Foreach
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream

object NseForecast {

  def main(args: Array[String]) = {

    val log = LoggerFactory.getLogger("NSEForecast")
    val conf = new SparkConf().setMaster("spark://cen-server:7077").setAppName("NSEForecast")
    //.set("spark.cores.max", "48")
    val sc = new SparkContext(conf)

    //val nifty50 = sc.textFile("hdfs://cen-server:5055/user/censpark/Nifty50.txt").collect

    //    val stockRDD = sc.textFile("hdfs://cen-server:5055/user/censpark/newNse/Jul/*/part*").map { l =>
      //    val s = l.split('|')
        //  val (md, id, stk, ts, pr) = (s(0).toInt, s(1).toInt, s(2), s(4).substring(0, 5), s(5).toFloat)
       //   ((md, stk, ts), (id, pr))
       // }.reduceByKey((x, y) => if (x._1 > y._1) x else y)

    // format minNSE: 3445623|IDEA|20150430||EQ|10:08|176.19846|15772.0

    //import scala.collection.mutable.ArrayBuffer
    //import java.io.PrintStream
    
    val wsize = 50
    val wslide = 25
    
    val tout = new PrintStream("time4")

    val stockRDD = sc.textFile("hdfs://cen-server:5055/user/censpark/minNse/Feb/part*").map { l =>
      val s = l.split('|')
      val (md, id, stk, ts, pr) = (s(2).toInt, s(0).toInt, s(1), s(5), s(6).toFloat)
      ((md, stk, ts), (id, pr))
    }.reduceByKey((x, y) => if (x._1 > y._1) x else y)

    val tsRDD = stockRDD.sortBy(k => k._1).map(v => (v._1, v._2._2))

    tsRDD.cache

    //val stocks = sc.textFile("hdfs://cen-server:5055/user/censpark/Nifty50.txt").collect
    
    //val stocks = List("HINDALCO","HCLTECH","RELIANCE","MARUTI","INFY","ITC","ICICIBANK","TCS","HDFC","ONGC",
      //  "TATAMOTORS","YESBANK", "BHARTIARTL", "BHEL", "LT", "AXISBANK", "SUNPHARMA","IDEA", "SBIN", "POWERGRID") 
    val stocks = List("SBIN")
    
        //tout.println(stocks)
    
    
    //************Repeated per company--lag 2 Skewing*************
    //val s = 2
    for (s <- 0 until stocks.length) {
    
      println(stocks(s)+ " Starting.......................")
      tout.println("************************"+stocks(s)+"**************************")
      val tsStckRDD = tsRDD.filter(v => (v._1._2 == stocks(s))).coalesce(48)
      println(tsStckRDD.count)
      val tsIndexRDD = tsStckRDD.zipWithIndex.map(v => (v._2, v._1._2))

      val rddlen = sc.broadcast(tsIndexRDD.count).value
      val rddpart = sc.broadcast(tsIndexRDD.partitions.length).value

      val partlen = sc.broadcast(rddlen / rddpart).value
      
      val lags0Initials = ArrayBuffer[Float]()
      val lags1Initials = ArrayBuffer[Float]()
      lags0Initials += 0
      lags1Initials += 0

      for (i <- 1 until rddpart) {
        lags0Initials += tsIndexRDD.lookup(i * partlen - 1)(0)
        lags1Initials += tsIndexRDD.lookup(i * partlen - 2)(0)
      }

      val lag = sc.broadcast(Array[Float](-1, -1)).value

      val lags0 = sc.broadcast(lags0Initials).value
      val lags1 = sc.broadcast(lags1Initials).value

      val tsSkewRDD = tsIndexRDD.map { i =>
        if (i._1 > 0 && lag(0) < 0) lag(0) = lags0((i._1 / partlen).toInt)
        if (i._1 > 1 && lag(1) < 0) lag(1) = lags1((i._1 / partlen).toInt)
        val k = (lag(1), lag(0), i._2)
        lag(1) = lag(0)
        lag(0) = i._2
        (i._1, k)
      }

      // Chop off the first two ....
      val tsSkewTrimedRDD = tsSkewRDD.filter(k => k._1 > 1)

      var allProjections = List((0.toLong, (0f, (0f, 0f, 0f))))

      var timeTot = 0L; 

      // ************************************Repeat per window of size 50 ....slide 25 ...******************************************
      for (i <- 0 to 20) {
    	  
        //println(stocks(s)+":estimating over window "+ i+1)
        
        val l = i * wslide
        val u = l + wsize

        val tsSampleRDD = tsSkewTrimedRDD.filter {
          _ match {
            case (k, v) => k >= l && k < u
            case _ => false //incase of invalid input
          }
        }.coalesce(4)

        var time = System.currentTimeMillis();

        val ar_2_EstimationRDD = tsSampleRDD.map { t =>
          val (i, (b, a, c)) = t
          (i, (c * a, c * b, a * b, a * a, b * b))
        }

        val (ca, cb, ab, a2, b2) = ar_2_EstimationRDD.map(v => v._2).reduce { (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
        }

        //AR(2) parameters
        val b1 = (cb * a2 - ca * ab) / (ab * ab + b2)
        val b0 = (ca + b1 * ab) / a2

        val mv_1_EstimationRDD = tsSampleRDD.map { t =>
          val (i, (b, a, c)) = t
          val c_cap = a * b0 + b * b1
          val e = c - c_cap
          (i, (c, e, c * e, e * e))
        }

        val (y, x, xy, x2) = mv_1_EstimationRDD.map(v => v._2).reduce { (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)
        }

        val mvCount = mv_1_EstimationRDD.count

        //MA(1) parameters
        val a1 = (xy - x * y / mvCount) / (x2 - x * x / mvCount)
        val a0 = y / mvCount - a1 * x / mvCount

        // Projection to next window slide ....

        //println(stocks(s)+":projecting over window "+ i+1)
        
        val tsProjectionSampleRDD = tsSkewTrimedRDD.filter {
          _ match {
            case (k, v) => k >= (i + 2) * wslide && k < (i + 3) * wslide
            case _ => false //incase of invalid input
          }
        }.coalesce(4)

        val (seed_i, (seed_b, seed_a, seed_c)) = tsProjectionSampleRDD.first

        var size = tsProjectionSampleRDD.count.toInt
        var p0 = seed_a
        var p1 = seed_c

        var p_cap = 0f
        var pe = 0f

        val projected = Array.fill[(Long, (Float, Float, Float))](size)(0, (0, 0, 0))

        for (j <- 0 to size - 1) {
          p_cap = p1 * b0 + p0 * b1
          pe = (p_cap - a0) / a1
          projected(j) = (seed_i + j, (p_cap, p_cap + a1 * pe, pe))
          p0 = p1
          p1 = p_cap
        }
        timeTot = System.currentTimeMillis() - time
        val projectRDD = tsProjectionSampleRDD.map(v => (v._1, v._2._3)).join(sc.parallelize(projected, 4))

        allProjections = allProjections ++ projectRDD.sortBy(k => k._1).collect        

      } // window loop
      //Save total time
      tout.println( timeTot/20 + " milliseconds for 20 windows of " + stocks(s) )
      allProjections.foreach(tout.println)
      //sc.parallelize(allProjections, 48).saveAsTextFile("hdfs://cen-server:5055/user/censpark/predictions/" + stocks(s))
    
    } //stock loop
    

  }

}