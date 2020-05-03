package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import com.redis.RedisClient
import es.prisa.derivado.constants.ProcessIds.EscrituraModeloRedis
import es.prisa.derivado.dao.ProcessDao
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class EscrituraModeloRedisProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {

  override val processName = EscrituraModeloRedis
  val pConfig = readEscrituraModeloRedisConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val IP_Redis = pConfig.IP
    val Puerto_Redis = pConfig.Puerto
    val json_bcio_path = pConfig.json_path

    val df = readParquetOrExit(pConfig.userVSscoreInputPath)

    val concatList = udf((l: Seq[String]) => l.mkString(","))

    writeLog(s"Obtaining offerScoreDF")
    val offerScoreDF = df.withColumn("offer_Str", concat(lit("{"), 'offerId, lit(": "))).
      withColumn("score_Str", concat('score, lit("}"))).
      withColumn("offerScore", concat('offer_Str, 'score_Str)).
      select('userId, 'pageId, 'campaignId, 'offerScore)

    writeLog(s"Obtaining UserJsonDF")
    val UserJsonDF = offerScoreDF.
      groupBy('userId, 'pageId, 'campaignId).
      agg(collect_list('offerScore) as "offerScore_List").
      withColumn("offerScore_Str", concatList('offerScore_List)).
      withColumn("campaignOffSco", concat('campaignId, lit(": [ "), 'offerScore_Str, lit(" ]"))).
      orderBy(asc("userId"), asc("pageId"), asc("campaignId")).
      groupBy('userId, 'pageId).
      agg(collect_list('campaignOffSco) as "campaignOffSco_List").
      withColumn("campaignOffSco_Str", concatList('campaignOffSco_List)).
      withColumn("pageCampOffSco", concat('pageId, lit(": { "), 'campaignOffSco_Str, lit(" }"))).
      orderBy(asc("userId"), asc("pageId")).
      groupBy('userId).
      agg(collect_list('pageCampOffSco) as "pageCampOffSco_List").
      withColumn("pageCampOffSco_Str", concatList('pageCampOffSco_List)).
      withColumn("userPageCampOffSco", concat(lit("[{ "), 'pageCampOffSco_Str, lit(" } ]"))).
      orderBy(asc("userId")).
      select('userId, 'userPageCampOffSco)

    writeLog(s"Saving data into Redis")

    val r = new RedisClient(IP_Redis, Puerto_Redis)
    UserJsonDF.rdd.collect().map{row => r.set(row.getString(0), row.getString(1))}
    UserJsonDF.rdd.collect().map{row => r.expire(row.getString(0),pConfig.ttl)} //Se establece la expiración de la key a 1 día.
  }
}

object EscrituraModeloRedisProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): EscrituraModeloRedisProcess = new EscrituraModeloRedisProcess(props, date, jobName, className)
}