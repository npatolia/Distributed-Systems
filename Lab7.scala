import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Lab7 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutils/")

    System.setProperty("hadoop.home.dir", "c:/winutils/")
    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val format = new java.text.SimpleDateFormat("yyyy-MM")


    val stores = sc.textFile("store.txt").
      map(line => (
        line.split(",")(0).trim().toInt,
        (line.split(",")(1).trim(),
          line.split(",")(2).trim(),
          line.split(",")(3).trim(),
          line.split(",")(4).trim().toInt,
          line.split(",")(5).trim(),
          line.split(",")(6).trim()
        )));

    val sales = sc.textFile("sales.txt").
      map(line => (
        line.split(",")(0).trim().toInt,
        (line.split(",")(1).trim(),
          line.split(",")(2).trim(),
          line.split(",")(3).trim().toInt,
          line.split(",")(4).trim().toInt
        )));

    val products = sc.textFile("product.txt").
      map(line => (
        line.split(",")(0).trim().toInt,
        (line.split(",")(1).trim(),
          line.split(",")(2).trim().toDouble
        )));

    val lineItems = sc.textFile("lineItem.txt").
      map(line => (
        line.split(",")(0).trim().toInt,
        (line.split(",")(1).trim().toInt,
          line.split(",")(2).trim().toInt,
          line.split(",")(3).trim().toInt
        )));

    val lineItemRemap = lineItems.map({case (listItemID, (salesID, productID, quantity)) => (productID , (salesID, quantity))})
    val lineItemJoinProduct = lineItemRemap.join(products)
    val dollarAmtPerSale = lineItemJoinProduct.map({case (productID,((salesID,quantity),(productName,productPrice))) =>
      (salesID, productPrice * quantity)})
    val salesJoinDolAmt = sales.join(dollarAmtPerSale)
    val salesRemap = salesJoinDolAmt.map({case (salesID,((date,time,storeID,customerID),salesAmt)) => (storeID, (date, salesAmt))})
    val storesJoinSales = stores.join(salesRemap)

    val result = storesJoinSales.map({case (storeID,((storeName, streetAddr,city,zipCode,state,phoneNumber), (date, sales))) =>
      ((format.parse(date.split("/")(0).trim() + "-" + date.split("/")(1).trim()) ,storeName) , sales)})



    val sums = result.reduceByKey((x,y) => x+y)

    val resultRemap = sums.map({case ((yearMonth,storeName),sales) => (yearMonth, (storeName, Math.round(sales)))})

    val groupByMonth = resultRemap.groupByKey()


    val sorted = groupByMonth.sortBy(x => x._1).take(1000)

    sorted.foreach(x=> println(format.format(x._1) + ", " + x._2.toList.sortBy(- _._2).take(10).mkString(", ")))


  }

}