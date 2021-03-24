import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

object Lab5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutils/")

    val storesFile = Source.fromFile("store.txt").getLines.toList
    var stores = new scala.collection.mutable.HashMap[Int, (String, String, String, Int, String, String)]
    for (line <- storesFile){
      stores += (line.split(",")(0).trim().toInt -> ((
          line.split(",")(1).trim(),
          line.split(",")(2).trim(),
          line.split(",")(3).trim(),
          line.split(",")(4).trim().toInt,
          line.split(",")(5).trim(),
          line.split(",")(6).trim())))
    }

    val salesFile = Source.fromFile("sales.txt").getLines.toList
    var sales = new scala.collection.mutable.HashMap[Int, (String, String, Int, Int)]
    for (line <- salesFile){

      sales += (line.split(",")(0).trim().toInt -> ((
          line.split(",")(1).trim(),
          line.split(",")(2).trim(),
          line.split(",")(3).trim().toInt,
          line.split(",")(4).trim().toInt)))
    }

    val productsFile = Source.fromFile("product.txt").getLines.toList
    var products = new scala.collection.mutable.HashMap[Int, (String, Double)]
    for (line <- productsFile){
      products += (line.split(",")(0).trim().toInt -> ((
          line.split(",")(1).trim(),
          line.split(",")(2).trim().toDouble)))
    }

    val lineItemsFile = Source.fromFile("lineItem.txt").getLines.toList
    var lineItems = new scala.collection.mutable.HashMap[Int, (Int, Int, Int)]
    for (line <- lineItemsFile){
        lineItems += (line.split(",")(0).trim().toInt -> ((
        line.split(",")(1).trim().toInt,
        line.split(",")(2).trim().toInt,
        line.split(",")(3).trim().toInt)))
    }

    val dollarAmtPerSale = new scala.collection.mutable.HashMap[Int, Double]
    for (item <- lineItems){
      if (dollarAmtPerSale isDefinedAt item._2._1){
        dollarAmtPerSale(item._2._1)  =  dollarAmtPerSale(item._2._1) + (item._2._3 * products(item._2._2)._2)
      } else {
        dollarAmtPerSale += item._2._1 -> products(item._2._2)._2
      }
    }

    val dollarAmtPerStore = new scala.collection.mutable.HashMap[Int, Double]
    for (item <- dollarAmtPerSale){
      if (dollarAmtPerStore isDefinedAt sales(item._1)._3){
        dollarAmtPerStore(sales(item._1)._3)  =  dollarAmtPerStore(sales(item._1)._3) + item._2
      } else {
        dollarAmtPerStore += sales(item._1)._3 -> item._2
      }
    }

    val result = ListBuffer[(String, Int, Double)]()
    for (item <- dollarAmtPerStore){
      result += ((stores(item._1)._5, item._1, item._2))
    }

    result.sortBy(_._1).foreach(x=> println(x._1 + ", " + x._2 + ", %.2f".format(x._3)))

    }

}