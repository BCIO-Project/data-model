package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.CalculateScore
import es.prisa.derivado.dao.ProcessDao
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}

class CalculateScoreProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {

  override val processName = CalculateScore
  val pConfig = readCalculateScoreConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName : String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // Modelo de clustering
    //val kmeansModel = KMeansModel.load(pConfig.modelInputPath)
    val kmeansModel = readKMeansOrExit(pConfig.modelInputPath)

    // Usuarios en clusters
    val clusteredData = readParquetOrExit( pConfig.clusteredDataInputPath).
      withColumn("userId", col("userId").cast(StringType)).persist()

    // Similitudes de contenidos de los últimos 30 días y de los nuevos contenidos
    val similarities = readParquetOrExit( pConfig.similarityInputPath)

    // ClicksRatio por cluster y contenido
    val clicksRatio = readParquetOrExit(pConfig.ctrByClusterInputPath)

    writeLog(s"#### Obtaining similaritiesVSClicksRatio ####")
    val similaritiesVSClicksRatio = clicksRatio.join(similarities,
      clicksRatio("contentId") === similarities("oldContent")).
      select(clicksRatio("cluster"), similarities("oldContent"),
        similarities("newContent"),
        clicksRatio("clicksRatio"),
        similarities("sim") as "similarityWithNewContent").
      na.fill(0, Seq("similarityWithNewContent")).
      groupBy("newContent", "cluster").
      agg(sum('similarityWithNewContent * 'clicksRatio) as "ctrNewContent").
      persist()

    val userList = clusteredData.select("userId").rdd.map(r => r(0).asInstanceOf[String]).collect.toList

    writeLog(s"#### Obtaining newContentsList ####")
    // Lista de nuevos contenidos
    val newContentsList = similaritiesVSClicksRatio.
      orderBy("newContent").
      select("newContent").
      distinct.
      rdd.map(r => r(0).asInstanceOf[String]).
      collect.toList

    // affinities para cada contenido
    writeLog(s"#### Obtaining affinitiesByNewContent ####")
    var affinitiesByNewContent = scala.collection.mutable.Map[String, List[Double]]()
    for (newContent <- newContentsList) {

      val afinityByCluster = similaritiesVSClicksRatio.
        filter('newContent === newContent).
        orderBy("cluster").
        select("ctrNewContent").
        persist.
        rdd.map(r => r(0).asInstanceOf[Double]).collect.toList

      affinitiesByNewContent += (newContent -> afinityByCluster)
    }

    val start = System.nanoTime()

    var numCluster = clusteredData.select('cluster).distinct.count.toInt
    var nUsers = 0
    var firstTime = true

    for( user <- userList) {
//      if (nUsers % 1 == 0) {
  //      println(nUsers + "/"+userList.size+" "+100*(nUsers/userList.size))
    //  }

      nUsers += 1


      var userVSscoreSeq:  Seq[Row] = Seq()


      val featuresUser : Vector = clusteredData.
        filter('userId === user).
        select("features").
        first.
        get(0).
        asInstanceOf[Vector]


      writeLog(s"#### Obtaining userVSscore ####")
      for(newContent <- newContentsList) {

        val afinityByCluster = affinitiesByNewContent(newContent)

        var score : Double = 0.0
        for(cluster <- 0 to numCluster - 1) {

          val centroid : Vector = kmeansModel.clusterCenters(cluster)
          val d = distance(featuresUser, centroid)

          score += d * afinityByCluster(cluster)

        }

        //val pageId = pageIdByNewContent(newContent)
        val newRow = Row(user, newContent, score)
        userVSscoreSeq = userVSscoreSeq :+ newRow
      }

      val userVSscore = spark.createDataFrame(
        spark.sparkContext.parallelize(userVSscoreSeq),
        StructType(
          List(
            StructField("userId", StringType, nullable=true),
            StructField("newContent", StringType, nullable=true),
            StructField("score", DoubleType, nullable=true)
          ))
      )

      writeLog(s"#### Obtaining formatedUserVSscore ####")
      // Adaptando formato de salida a los requisitos del cliente
      // Separando newContent en: pageId, campaign y offert
      val formatedUserVSscore = userVSscore.
        withColumn("pageId", split(col("newContent"), "\\_").getItem(0)).
        withColumn("campaignId", split(col("newContent"), "\\_").getItem(1)).
        withColumn("offerId", split(col("newContent"), "\\_").getItem(2)).
        select("userId", "pageId", "campaignId", "offerId", "score")


      // Guardando resultado
      if (firstTime) {
        firstTime = false
        writeData(formatedUserVSscore, pConfig.userVSscoreOutputPath)
      } else {
        formatedUserVSscore.write.mode("append").parquet( pConfig.userVSscoreOutputPath)
      }
    }
  }

  def distance(a : Vector, b : Vector) : Double = {
    var total : Double = 0
    for (i <- 0 to a.size - 1) {
      total += Math.pow(a(i) - b(i),2)
    }
    if(total > 0){
      return 1 / Math.sqrt(total)
    }else {
      return 0.0
    }
  }
}

object CalculateScoreProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): CalculateScoreProcess = new CalculateScoreProcess(props, date, jobName, className)
}

