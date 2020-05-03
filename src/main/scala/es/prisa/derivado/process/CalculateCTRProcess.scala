package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.CalculateCTR
import es.prisa.derivado.dao.ProcessDao
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class CalculateCTRProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {

  override val processName = CalculateCTR
  val pConfig = readCalculateCTRConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName : String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val clustering = readParquetOrExit(pConfig.clusteredDataInputPath)
    val clicksVSimpressionsLast30 = readParquetOrExit( pConfig.ctrInputPath).
      withColumn("contentId", concat('pageId, lit("_"), 'campaignId, lit("_"), 'offerId)).
      select("userId", "contentId", "clicks", "impressions", "CTR")

    writeLog(s"#### Obtaining ctrByCluster ####")
    val ctrByCluster = clustering.
      join(clicksVSimpressionsLast30, clustering("userId") === clicksVSimpressionsLast30("userId"), "left").
      select(clustering("userId"), clustering("cluster"),
        clicksVSimpressionsLast30("contentId"),
        clicksVSimpressionsLast30("impressions"), clicksVSimpressionsLast30("clicks"),
        clicksVSimpressionsLast30("CTR")).
      groupBy("cluster", "contentId").
      agg(count("*") as "totalContentsByCluster",
        sum("impressions") as "totalImpresions",
        sum("clicks") as "totalClicks").
      withColumn("clicksRatio", col("totalClicks")/col("totalImpresions"))

    val ctrByClusterClean = ctrByCluster.na.fill(0, Seq("totalClicks", "clicksRatio"))

    writeData(ctrByClusterClean, pConfig.ctrByClusterOutputPath)
  }

}

object CalculateCTRProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): CalculateCTRProcess = new CalculateCTRProcess(props, date, jobName, className)
}

