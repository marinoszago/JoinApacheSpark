import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.reflect.ClassTag


object join {

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }


  def main(args: Array[String]) {

    // Create spark configuration and spark context
    val conf = new SparkConf().setAppName("Join Relations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ss: SparkSession =
      SparkSession
        .builder()
        .appName("Join Relations")
        .config("spark.master", "local")
        .getOrCreate()

    import ss.implicits._


    val currentDir = System.getProperty("user.dir")  // get the current directory
    val inputFile = "file://" + currentDir + "/example2.txt"
    val outputDir = "file://" + currentDir + "/output"


    println("reading from input file: " + inputFile)

    val myTextFile = sc.textFile(inputFile).cache()


    /*Rdd Join*/

    val R_RDD = myTextFile.filter(pairsKV => pairsKV.contains("R")).flatMap(line => line.split("/n")).map(line => line.split(",")).map(pairKV => (pairKV(1).toInt,(pairKV(0),pairKV(2)))).groupByKey().mapValues(_.toList.mkString(""))

    //R_RDD.take(10).foreach(println)

    val S_RDD = myTextFile.filter(pairsKV => pairsKV.contains("S")).flatMap(line => line.split("/n")).map(line => line.split(",")).map(pairKV => (pairKV(1).toInt,(pairKV(0),pairKV(2)))).groupByKey().mapValues(_.toList.mkString(""))
    //S_RDD.take(10).foreach(println)

    println("Rdd Time")
    val joinRDD = time{R_RDD.join(S_RDD)}


    /*joinRDD.coalesce(1).map { case (key, value) => var line = key.toString + "," + value.toString
      line}.saveAsTextFile(outputDir+"/RddJoin")*/


/*-----------------------------------------------------------------------------------------------------------*/

    /*DATA SET JOIN*/



    val R_dataset = ss.createDataset(R_RDD).selectExpr("_1 as Key" , "_2 as ValueR")

    //R_dataset.show()
    val S_dataset = ss.createDataset(S_RDD).selectExpr("_1 as Key" , "_2 as ValueS")
    //S_dataset.show()

    println("Dataset Time")
    val JoinDataset = time{R_dataset.join(S_dataset,"Key")}


    //JoinDataset.show()

    //JoinDataset.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter","\t").save(outputDir+"/datasetJoin")


/*-----------------------------------------------------------------------------------------------------------------------------*/

    /*DATAFRAME JOIN*/

    /*val dataframe = ss.read.option("header","false").csv("example1.txt").selectExpr("_c0 as Key","_c1 as Value1 ","_c2 as Value2")
    dataframe.createOrReplaceTempView("myTable")*/


    val dataframeR = R_RDD.toDF("Key","ValueR").repartition(1).createTempView("DFR")
    val dataframeS = S_RDD.toDF("Key","ValueS").repartition(1).createTempView("DFS")

    println("Dataframe Time")
    val joinDataFrame = time{ss.sqlContext.sql("select coalesce (DFR.Key,DFS.Key) as Key ,DFR.ValueR,DFS.ValueS from DFR join DFS on DFR.Key = DFS.Key ")}

    //joinDataFrame.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter","\t").save(outputDir+"/dataframeJoin")


    /*-----------------------------------------------------------------------------------------------------------------------------*/
    /*Optimized RDD Map-Side JOIN*/
    println("Optimized Time")

    val smallRDD = sc.broadcast(R_RDD.collect().toMap)
    val JoinRDD = time{S_RDD.flatMap({ case(key, value) =>
      smallRDD.value.get(key).map { otherValue =>
        (key, (value, otherValue))
      }
    })}
    val countJoin = JoinRDD.count()
    println(countJoin.toLong)


    myTextFile.unpersist()
      sc.stop()


  }

}

