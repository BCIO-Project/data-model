package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.GetInfoAPI
import es.prisa.derivado.dao.ProcessDao
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json._


class GetInfoAPIProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {

  override val processName = GetInfoAPI
  val pConfig = readGetInfoAPIConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName : String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val url_api_token = pConfig.url_api_token
    val header_auth = pConfig.header_auth
    val url_api_campaigns = pConfig.url_api_campaigns

    val jsonToken = API_TokenResult(url_api_token)
    val bearer_token = get_Token(jsonToken)
    writeLog("#### Token obtained ####")

    writeLog("#### Calling the API ####")
    val jsonString = API_result(header_auth, bearer_token, url_api_campaigns)
    val json_parsed = API_result_parse(jsonString)

    writeLog("#### Loaded data ####")

    var DF_Info_Campaigns = Seq.empty[(Int, Int, Int, List[String], java.sql.Timestamp, java.sql.Timestamp)].
      toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags", "From", "To")

    val fecha_cts = pConfig.fecha_cts

    writeLog("#### Union ####")
    for( campaign <- json_parsed) {
      writeLog(s"${get_Campaign_Info(campaign, fecha_cts)}")
      DF_Info_Campaigns = DF_Info_Campaigns.union(get_Campaign_Info(campaign, fecha_cts))
    }

    DF_Info_Campaigns.show(false)

    writeData(DF_Info_Campaigns, pConfig.output_path)
  }

  def API_TokenResult( url_api:String ) : String = {

    val httpPost = new HttpPost(url_api)
    val client  = new DefaultHttpClient

    var response   = client.execute(httpPost)    // Se hace la llamada. Devuelve un CloseableHttpResponse.
    var entity     = response.getEntity()       // Devuelve un HTTPEntity.
    var content    = entity.getContent()        // Devuelve un InputStream con el contenido.
    var jsonToken = IOUtils.toString(content); //convierte el InputStream en un String.

    // Se cierra el flujo HTTP.
    client.getConnectionManager.shutdown

    return jsonToken
  } // API_result

  def get_Token( jsonToken:String ) : String = {

    val startToken = jsonToken.indexOf("token") + 8
    val endToken = jsonToken.lastIndexOf("\"}")
    val bearer_token = "Bearer " + jsonToken.substring(startToken, endToken)

    bearer_token
  }

  def API_result( header_auth:String, bearer_token:String, url_api:String ) : String = {

    val httpGet = new HttpGet(url_api)
    val client  = new DefaultHttpClient

    // Se establece la cabecera "Authorization" para la solicitud.
    httpGet.setHeader(header_auth, bearer_token)

    var response   = client.execute(httpGet)    // Se hace la llamada. Devuelve un CloseableHttpResponse.
    var entity     = response.getEntity()       // Devuelve un HTTPEntity.
    var content    = entity.getContent()        // Devuelve un InputStream con el contenido.
    var jsonString = IOUtils.toString(content); //convierte el InputStream en un String.

    // Se cierra el flujo HTTP.
    client.getConnectionManager.shutdown

    return jsonString
  }

  def API_result_parse( jsonString:String) : List[Map[String, Any]] = {

    val json_parsed_aux = JSON.parseFull(jsonString).asInstanceOf[Some[List[Map[String, Any]]]]
    val json_parsed = json_parsed_aux.getOrElse(List(Map()).asInstanceOf[List[Map[String, Any]]])

    return json_parsed
  }

  def get_Campaign_Info( campaign_in: Map[String, Any], fecha_cts: String)(implicit spark: SparkSession) : DataFrame = {

    import spark.implicits._

    var DF_Campaign_Offer = Seq.empty[(Int, Int, Int, List[String], java.sql.Timestamp, java.sql.Timestamp)].toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags", "From", "To")
    var DF_Campaign_Offer_aux = Seq.empty[(Int, Int, Int, List[String], java.sql.Timestamp, java.sql.Timestamp)].toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags", "From", "To")

    val campaign    = campaign_in
    val page_id     = campaign_in( "pageId").asInstanceOf[Double].toInt
    val campaign_id = campaign_in( "id").asInstanceOf[Double].toInt
    val from        = campaign_in( "from").asInstanceOf[String]
    val to          = campaign_in( "to").asInstanceOf[String]
    val offer_list  = campaign_in( "offers").asInstanceOf[List[Map[String, Any]]]

    // Si el rango de fecha de la campaña contiene la fecha de ejecución,
    // se almacena la oferta de la iteración actual en un DF auxiliar.

    val from_substr = from.substring(0, 10)
    val to_substr = to.substring(0, 10)

    if ( from_substr <= fecha_cts && to_substr >= fecha_cts ){

      for( offer <- offer_list) {

        var offer_tags = List[String]()
        var offer_id = offer( "id").asInstanceOf[Double].toInt
        var tags = offer( "tags").asInstanceOf[List[Map[String, Any]]]

        for ( tag <- tags){

          var tag_id = tag( "name").asInstanceOf[String]
          offer_tags = tag_id :: offer_tags
        }

        DF_Campaign_Offer_aux = Seq((page_id, campaign_id, offer_id, offer_tags, from, to)).
          toDF("Page_ID", "Campaign_ID", "Offer_ID", "Offer_Tags", "From", "To")

        // Se unifican en un mismo DF todas las ofertas de la lista.
        DF_Campaign_Offer = DF_Campaign_Offer.union(DF_Campaign_Offer_aux)

      } // for( offer <- offer_list)
    }

    return DF_Campaign_Offer
  }

}


object GetInfoAPIProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): GetInfoAPIProcess = new GetInfoAPIProcess(props, date, jobName, className)
}