package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.FormattingData
import es.prisa.derivado.dao.ProcessDao
import es.prisa.derivado.util.IngestionType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class FormattingDataProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {
  override val processName = FormattingData
  val pConfig = readFormattingDataConfig()

  override def executeProcess(props: Properties, date: LocalDate, jobType: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val df = readParquetOrExit(pConfig.userVStagsInputPath).
             filter(!'tag.isNull)
        // Estandariza los nombres de las tags

    val standarizeTagName = udf( ( a:String) => a.toLowerCase.replaceAll("á", "a").
      replaceAll("é", "e").
      replaceAll("í", "i").
      replaceAll("ó", "o").
      replaceAll("ú", "u").
      replaceAll(" ","_"))

    writeLog(s"#### Creating array with standarized labels and alphabetically ordered")
    val allTags = df.
      select( 'tag).
      distinct.
      rdd.
      map{ r => r(0)}.
      collect().
      map( x => x.
        toString().
        toLowerCase().
        replaceAll("á", "a").
        replaceAll("é", "e").
        replaceAll("í", "i").
        replaceAll("ó", "o").
        replaceAll("ú", "u").
        replaceAll(" ","_"))

    writeLog(s"#### Creating a new variable from the tag variable with standard format")
    val dfFormatedTags = df.withColumn("standardTag", standarizeTagName( 'tag))

    writeLog(s"#### Creating Dataframe with the structure user x tags")
    val dfFormatedTagsPivot = dfFormatedTags.
      groupBy("userId").
      pivot("standardTag", allTags).
      agg( first( 'CTR)).
      na.fill(0.0).cache()

    writeLog(s"#### dfFormatedTagsPivot: " + dfFormatedTagsPivot.count)

    writeData(dfFormatedTagsPivot, pConfig.formattedOutputPath)
  }
}

object FormattingDataProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): FormattingDataProcess = new FormattingDataProcess(props, date, jobName, className)
}