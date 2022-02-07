package config

import utils.CommandLineParser

import java.time.{LocalDate, Year}

sealed trait AppConfig

final case class EnricherAppConfig(readerConfig: ReaderConfig, behavior: Behavior, dateRange: CodMonthRange) extends AppConfig

final case class AnalyzerAppConfig(readerConfig: ReaderConfig, method: MethodAnalyzer, behavior: Behavior, gameId: String, dateRange: CodMonthRange) extends AppConfig

object AppConfig extends CommandLineParser {
  type ExMsg = String

  def getGameIdFromOpts(mapOpts: Map[String, String]): Either[String, String] = {
    val gameIdOpt = mapOpts.get("gameId")
    gameIdOpt match {
      case Some(gameId) => Right(gameId)
      case _ => Left("gameId was not provided")
    }
  }

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
    val gameIdOpt = getGameIdFromOpts(mapOptions)
    val methodOpt = MethodAnalyzer.getAnalyzerMethodFromOpts(mapOptions)
    val dateRangeOpt = CodMonthRange.getDateRangeFromOpts(mapOptions)

    (readerConfigOpt, methodOpt, behaviorOpt, gameIdOpt, dateRangeOpt) match {
      case (Right(readerConfig), Right(Retention(startMonth, idleTime)), _, Right(gameId) , _ ) =>
        def printMonth(month: Int): String =
          if (month < 10) s"0$month" else s"$month"

        val initialDate = LocalDate.of(startMonth.getYear, startMonth.getMonth, 1)
        val days = startMonth.getMonth.length(Year.isLeap(startMonth.getYear))
        val finalDate = initialDate.plusDays(days+idleTime)
        val range = CodMonthRange(s"${initialDate.getYear}${printMonth(initialDate.getMonthValue)}", s"${finalDate.getYear}${printMonth(finalDate.getMonthValue)}")
        Right(AnalyzerAppConfig(readerConfig, Retention(startMonth, idleTime), Daily, gameId, range))
      case (Right(readerConfig), Right(method), Right(behavior), Right(gameId), Right(codMonthRange)) =>
        Right(AnalyzerAppConfig(readerConfig, method, behavior, gameId, codMonthRange))
      case _ =>
        val exLoadAppConfMsg = List(
          readerConfigOpt,
          methodOpt,
          behaviorOpt,
          gameIdOpt,
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