package es.prisa.derivado.main

import java.io.FileReader
import java.time.{LocalDate, ZoneId}
import java.util.Properties

import es.prisa.common.arq.option.BasicJobOption
import es.prisa.common.arq.template.BasicDefaultTemplate
import es.prisa.common.literals.config.CommonConfig.litRootAppConfig
import es.prisa.common.logs.LogLevel
import es.prisa.derivado.constants.DerivadoConstants._
import es.prisa.derivado.constants.ProcessIds
import es.prisa.derivado.constants.ProcessIds._
import es.prisa.derivado.process._
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


/**
  * Template para Crm01
  *
  * @author INDRA
  */

case class IngestionParams(val date: LocalDate, val configpath: String, val sourceType: String)

object AppMain extends BasicDefaultTemplate[IngestionParams] {

  /** Se define el nombre de programa */
  //override protected def defineProgramName(): String = "Ingestion"
  override protected def defineProgramName(): String = APPNAME

  /** Se declaran las opciones de selección de jobs */
  override protected def defineListProcessType(): Array[BasicJobOption] = ProcessIds.values.toArray

  /** Configuración de argumentos */
  override protected var parameters: OptionParser[IngestionParams] = new OptionParser[IngestionParams](programName) {
    head(programName, "Spark")

    opt[java.util.Calendar]('d', "date")
      .action((dateParam, params) => params.copy(date = dateParam.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .text("Date is required")
    opt[String]('s', "source").required()
      .action((sourceType, params) => params.copy(sourceType = sourceType))
      .text("Source Type is required and must be in the config file.")

    opt[java.util.Calendar]('y', "year")
    opt[String]('c', "conf").required().action((configPath, params) => params.copy(configpath = configPath)).text("The config path is required")
    help("help").text(s" -d YYYY-MM-DD")
  }

  /**
    * Función principal.
    *
    * @param args Argumentos del usuario para la ejecución del programa.
    */
  override def main(args: Array[String]): Unit = {
    paramsProcess(args, IngestionParams(date = LocalDate.now, configpath = litRootAppConfig, sourceType = ""))
  }

  /**
    * Ejecutamos el proceso seleccionado mediante el argumento "source".
    *
    * Ejecutamos proceso directamente ya que no tenemos lógica en jobs, si existiera algún flujo
    * o dependencias entre dos o más procesos implementaríamos dicha lógica en el Job.
    *
    * @param props   Propiedades leidas del fichero de propiedad indicado por argumentos.
    * @param date    Fecha indicada al job para su ejecución
    * @param processName Nombre identificador del job/proceso que se pretende ejecutar.
    *                Debido a un problema de compatibilidad, En este caso el formato es
    *                proceso-fuente.
    *
    * @param spark   Contexto de Spark.
    */
  override protected def executeJob(props: Properties, date: LocalDate, processName: String)(implicit spark: SparkSession): Unit = {

    writeLog(s"processName param: $processName")
    writeLog(s"date param: ${date.toString}")

    /* Due a compatibility issues process name and source name are got from the same parameter SourceType instead of having to params...*/
    val process = processName match {
      case GetInfoAPI.name              => GetInfoAPIProcess(props, date, GetInfoAPI.name, GetInfoAPI.getClass.getCanonicalName)
      case EscrituraModeloRedis.name    => EscrituraModeloRedisProcess(props, date, EscrituraModeloRedis.name, EscrituraModeloRedis.getClass.getCanonicalName)
      case Modelo30DaysCampaignInfoCTR.name => Modelo30DaysCampaignInfoCTRProcess(props, date, Modelo30DaysCampaignInfoCTR.name, Modelo30DaysCampaignInfoCTR.getClass.getCanonicalName)
      case FormattingData.name              => FormattingDataProcess(props, date, FormattingData.name, FormattingData.getClass.getCanonicalName)
      case PredictClustering.name              => PredictClusteringProcess(props, date, PredictClustering.name, PredictClustering.getClass.getCanonicalName)
      case TrainClustering.name              => TrainClusteringProcess(props, date, TrainClustering.name, TrainClustering.getClass.getCanonicalName)
      case ContentSimilarity.name            => ContentSimilarityProcess(props, date, ContentSimilarity.name, ContentSimilarity.getClass.getCanonicalName)
      case CalculateCTR.name              => CalculateCTRProcess(props, date, CalculateCTR.name, CalculateCTR.getClass.getCanonicalName)
      case CalculateScore.name              => CalculateScoreProcess(props, date, CalculateScore.name, CalculateScore.getClass.getCanonicalName)
      case _ => throw new UnsupportedOperationException(processName)
    }
    writeLog("Source to Process: " + processName)
    process.executeProcess(props, date, processName)
  }


  /**
    * Leemos los parámetros y recuperamos el fihero de configuración.
    *
    * @param args Argumentos recibidos desde la función main.
    * @param params Propiedades leidas del fichero de propiedad indicado por argumentos.
    */
  override protected def paramsProcess(args: Array[String], params: IngestionParams): Unit = parameters.parse(args,params) match {
    case Some(params) => {
      val props:Properties = new Properties()
      props.load(new FileReader(params.configpath))

      writeLog(s"---> Fichero propiedades leido (${params.configpath})")
      writeLog("---> params.source (jobType)=" + params.sourceType )
      executeJob(props, params.date, params.sourceType)
    }
    case None => writeLog("Wrong params!", LogLevel.ERROR)
  }
}