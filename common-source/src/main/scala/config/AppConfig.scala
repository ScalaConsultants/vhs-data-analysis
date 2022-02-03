package config

import utils.CommandLineParser

sealed trait AppConfig

final case class EnricherAppConfig(readerConfig: ReaderConfig, behavior: Behavior, dateRange: CodMonthRange) extends AppConfig

final case class AnalyzerAppConfig(readerConfig: ReaderConfig, method: MethodAnalyzer, behavior: Behavior, dateRange: Option[CodMonthRange]) extends AppConfig

object AppConfig extends CommandLineParser {
  type ExMsg = String

  def loadEnrichedConfig(args: Array[String]): Either[ExMsg, EnricherAppConfig] = {
    val mapOptions = getOpts(args)
    val readerConfigOpt = ReaderConfig.getReaderConfigFromOpts(mapOptions)
    val behaviorOpt = Behavior.getBehaviorFromOpts(mapOptions)
    val dateRangeOpt = CodMonthRange.getDateRangeFromOpts(mapOptions)

    (readerConfigOpt, behaviorOpt, dateRangeOpt) match {
      case (Right(readerConfig), Right(behavior), Right(codMonthRange)) =>
        Right(EnricherAppConfig(readerConfig, behavior, codMonthRange))
      case _ =>
        val exLoadAppConfMsg = List(
          readerConfigOpt,
          behaviorOpt,
          dateRangeOpt
        )
        .collect{
          case Left(exMsg) => exMsg
        }
        .mkString(", ")

        Left(exLoadAppConfMsg)
    }
  }

  def loadAnalyzerConfig(args: Array[String]): Either[ExMsg, AnalyzerAppConfig] = {
    val mapOptions = getOpts(args)
    val readerConfigOpt = ReaderConfig.getReaderConfigFromOpts(mapOptions)
    val behaviorOpt = Behavior.getBehaviorFromOpts(mapOptions)
    val methodOpt = MethodAnalyzer.getAnalyzerMethodFromOpts(mapOptions)
    val dateRangeOpt = CodMonthRange.getDateRangeFromOpts(mapOptions)

    (readerConfigOpt, methodOpt, behaviorOpt, dateRangeOpt) match {
      case (Right(readerConfig), Right(method), Right(behavior), Left(e) ) =>
        method match {
          case _: Retention => Right(AnalyzerAppConfig(readerConfig, method, behavior, None))
          case _ => Left(e)
        }
      case (Right(readerConfig), Right(method), Right(behavior), Right(codMonthRange)) =>
        Right(AnalyzerAppConfig(readerConfig, method, behavior, Some(codMonthRange)))
      case _ =>
        val exLoadAppConfMsg = List(
          readerConfigOpt,
          methodOpt,
          behaviorOpt,
          dateRangeOpt
        )
          .collect {
            case Left(exMsg) => exMsg
          }
          .mkString(", ")

        Left(exLoadAppConfMsg)
    }
  }
}