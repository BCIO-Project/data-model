package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.Modelo30DaysCampaignInfoCTR
import es.prisa.derivado.dao.ProcessDao
import org.apache.spark.sql.SparkSession
import com.samelamin.spark.bigquery._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

class Modelo30DaysCampaignInfoCTRProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {

  override val processName = Modelo30DaysCampaignInfoCTR
  val pConfig = readModelo30DaysCampaignInfoCTRConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val project_id = pConfig.project_id
    val dataset = pConfig.dataset
    val table = pConfig.table
    val gs_bucket = pConfig.gs_bucket
    val location = pConfig.bqDatasetLocation

    spark.sqlContext.setBigQueryGcsBucket(gs_bucket)
    spark.sqlContext.setBigQueryProjectId(project_id)
    spark.sqlContext.setBigQueryDatasetLocation(location)
    spark.sqlContext.useStandardSQLDialect(true)

    var query = s"""select * from `${project_id}.${dataset}.${table}`
              where  date(createdAt) > date_add(current_date(), interval -1 MONTH)
              and    date(createdAt) <= current_date()
              and    userId is not null
              and    campaignId is not null
              and    offerId is not null
              and    createdAt is not null"""
    writeLog(query)

    val eventsTable = spark.sqlContext.bigQuerySelect(query)

    val allUsers = eventsTable.select('userId).distinct

    /*val eventsTable = spark.sqlContext.read.format("com.samelamin.spark.bigquery").
      option("tableReferenceSource",s"${project_id}:${dataset}.${table}").load()
*/

    writeLog(s"#### Obtaining eventsCount ####")
    val eventsCount = eventsTable.
      groupBy('userId, 'campaignId, 'offerId, 'eventType).
      agg(count('eventType).alias("total_events")).
      select( 'userId, 'campaignId, 'offerId, 'eventType, 'total_events)

    writeLog(s"#### Obtaining eventsClicks ####")
    val eventsClicks = eventsCount.
      where( 'eventType === "click").
      withColumn("clicks", 'total_events).
      select( 'userId, 'campaignId, 'offerId, 'eventType, 'clicks)

    writeLog(s"#### Obtaining eventsImpressions ####")
    val eventsImpressions = eventsCount.
      where( 'eventType === "impression").
      withColumn("impressions", 'total_events).
      select( 'userId, 'campaignId, 'offerId, 'eventType, 'impressions)

    writeLog(s"#### Obtaining eventsCTR_aux ####")
    val eventsCTR_aux = eventsClicks.
      join( eventsImpressions, Seq("userId", "campaignId", "offerId"), "inner").
      select( eventsClicks.col("userId"), eventsClicks.col("campaignId"), eventsClicks.col("offerId"),
        eventsClicks.col("clicks"), eventsImpressions.col("impressions")).
      withColumn( "CTR", 'clicks/'impressions)

    var path30DaysExists = List[String]()
    for(path <- pConfig.inputPaths30Days){
      val path_aux = new Path(path)
      val fs = path_aux.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if ( fs.exists(path_aux) ){
        path30DaysExists = path :: path30DaysExists
      }
    }

    var campaignInfo30Days = Seq.empty[(Int, Int, Int, List[String])].toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags")

    if (!path30DaysExists.isEmpty){
        campaignInfo30Days = readMultipleParquetFiles(path30DaysExists)
    }else{
        writeLog(s"There is no data for last 30 days.")
        System.exit(1)
    }

    var newCampaignInfo = Seq.empty[(Int, Int, Int, List[String])].toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags").
      select('Page_ID.alias("pageId"), 'Campaign_ID.alias("campaignId"), 'Offer_ID.alias("offerId"), 'Offer_Tags.alias("offerTags"))

    val path_aux = new Path(pConfig.newCampaignInfoInputPath)
    val fs = path_aux.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if ( fs.exists(path_aux) ){
      // Se obtiene la información de campañas de la fecha de ejecución.
      newCampaignInfo = readParquetOrExit(pConfig.newCampaignInfoInputPath).
        select('Page_ID.alias("pageId"), 'Campaign_ID.alias("campaignId"), 'Offer_ID.alias("offerId"), 'Offer_Tags.alias("offerTags"))
    }

    val campaignInfo30Days_dedup = campaignInfo30Days.
      select('Page_ID.alias("pageId"), 'Campaign_ID.alias("campaignId"), 'Offer_ID.alias("offerId"), 'Offer_Tags.alias("offerTags")).
      groupBy('pageId, 'campaignId, 'offerId).
      agg(first('offerTags) as "offerTags")

    val allUsersOffers = campaignInfo30Days_dedup.crossJoin(allUsers)
    val eventsCTR = allUsersOffers.join(eventsCTR_aux,
                    Seq("userId", "campaignId", "offerId"), "left").
                    select( eventsCTR_aux.col("userId"), 'pageId, 'campaignId, 'offerId, 'offerTags, 'clicks, 'impressions, 'CTR).
                    na.fill(0, Seq("CTR", "clicks", "impressions"))
    
    val campaignInfo30Days_tags = campaignInfo30Days_dedup.
      withColumn( "tag", explode_outer( 'offerTags)).
      withColumn( "execDate", current_date()).
      select('pageId, 'campaignId, 'offerId, 'tag, 'execDate)

    val newCampaignInfo_tags = newCampaignInfo.
      withColumn( "tag", explode_outer( 'offerTags)).
      withColumn( "execDate", current_date()).
      select('pageId, 'campaignId, 'offerId, 'tag, 'execDate)

    val eventsCTR_tags = eventsCTR

    val eventsUserId_tags = eventsCTR.
      withColumn( "tag", explode_outer( 'offerTags)).
      select('userId, 'clicks, 'impressions, 'tag).
      groupBy('userId, 'tag).
      agg(sum('clicks)/sum('impressions) as "CTR").
      withColumn( "execDate", current_date()).
      select('userId, 'tag, 'CTR, 'execDate)

    if (campaignInfo30Days_tags.rdd.isEmpty || newCampaignInfo_tags.rdd.isEmpty ||
      eventsCTR_tags.rdd.isEmpty || eventsUserId_tags.rdd.isEmpty) {
      
      writeLog("#### ERROR: One or more Dataframes are empty ####")
      System.exit(1)
    }else {
      writeData(campaignInfo30Days_tags, pConfig.campaignInfo30DaysOutputPath)
      writeData(newCampaignInfo_tags, pConfig.newInfoOutputPath)
      writeData(eventsCTR_tags, pConfig.eventsCTROutputPath)
      writeData(eventsUserId_tags, pConfig.eventsUserIdOutputPath)
    }
  }
}

object Modelo30DaysCampaignInfoCTRProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): Modelo30DaysCampaignInfoCTRProcess = new Modelo30DaysCampaignInfoCTRProcess(props, date, jobName, className)
}