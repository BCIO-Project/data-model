package es.prisa.derivado.constants

import es.prisa.derivado.util.IngestionType

object ProcessIds extends {
  object GetInfoAPI extends IngestionType(name = "GetInfoAPI")
  object EscrituraModeloRedis extends IngestionType(name = "EscrituraModeloRedis")
  object Modelo30DaysCampaignInfoCTR extends IngestionType(name = "Modelo30DaysCampaignInfoCTR")
  object FormattingData extends IngestionType(name = "FormattingData")
  object PredictClustering extends IngestionType(name = "PredictClustering")
  object TrainClustering extends IngestionType(name = "TrainClustering")
  object ContentSimilarity extends IngestionType(name = "ContentSimilarity")
  object CalculateCTR extends IngestionType(name = "CalculateCTR")
  object CalculateScore extends IngestionType(name = "CalculateScore")
  val values = Seq(GetInfoAPI, EscrituraModeloRedis, Modelo30DaysCampaignInfoCTR, FormattingData,
    PredictClustering, TrainClustering, ContentSimilarity)

}