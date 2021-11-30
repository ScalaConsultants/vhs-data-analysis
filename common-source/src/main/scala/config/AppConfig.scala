package config

import utils.CommandLineParser
import CodMonthRange.codMonthRegex

final case class AppConfig(readerConfig: ReaderConfig, behavior: Behavior, dateRange: CodMonthRange)

object AppConfig extends CommandLineParser {
  type ExMsg = String

  private def getMongoReaderConfigFromOpts(mapOpts: Map[String, String]): Option[MongoReaderConfig] = {
    val mongoReaderConfigOpts = (mapOpts.get("mongoUri"), mapOpts.get("database"), mapOpts.get("collection"))
    mongoReaderConfigOpts match {
      case (Some(mongoUri), Some(database), Some(collection)) => Some(MongoReaderConfig(mongoUri, database, collection))
      case _ => None
    }
  }

  private def getLocalFileReaderConfigFromOpts(mapOpts: Map[String, String]): Option[LocalFileReaderConfig] = {
    val localFileReaderConfigOpts = (mapOpts.get("mainPath"), mapOpts.get("folderName"))
    localFileReaderConfigOpts match {
      case (Some(mainPath), Some(folderName)) => Some(LocalFileReaderConfig(mainPath, folderName))
      case _ => None
    }
  }

  private def getReaderConfigFromOpts(mapOpts: Map[String, String]): Either[ExMsg, ReaderConfig] = {
    val mongoReaderConfig = getMongoReaderConfigFromOpts(mapOpts)
    val localFileReaderConfig = getLocalFileReaderConfigFromOpts(mapOpts)

    (mongoReaderConfig, localFileReaderConfig) match {
      case (Some(mongoReaderConf), None) => Right(mongoReaderConf)
      case (None, Some(localFileReaderConf)) => Right(localFileReaderConf)
      case (Some(_), Some(_)) => Left("ambiguous readerConfig")
      case (None, None) => Left("readerConfig was not provided")
    }
  }

  private def getDateRangeFromOpts(mapOpts: Map[String, String]): Either[ExMsg, CodMonthRange] = {
    val (fromDateOpt, toDateOpt) = (mapOpts.get("fromDate"), mapOpts.get("toDate"))
    (fromDateOpt, toDateOpt) match {
      case (Some(codMonthRegex(fromCodMonth)), Some(codMonthRegex(toCodMonth))) =>  Right(CodMonthRange(fromCodMonth, toCodMonth))
      case _ => Left("invalid dateRange format")
    }
  }

  /*
    app behavior can be daily, monthly or maybe both(both only works for enriched module)
    appBehavior by default is daily if it is not provided
  */
  private def getBehaviorFromOpts(mapOpts: Map[String, String]): Either[ExMsg, Behavior] = {
    val behaviorOpt = mapOpts.get("behavior")
    behaviorOpt match {
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Daily.label) => Right(Daily)
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Monthly.label) => Right(Monthly)
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Both.label) => Right(Both)
      case None => Right(Daily)
      case _ => Left("app behavior introduced doesn't exist")
    }
  }

  def load(args: Array[String]): Either[ExMsg, AppConfig] = {
    val mapOptions = getOpts(args)
    val readerConfigOpt = getReaderConfigFromOpts(mapOptions)
    val behaviorOpt = getBehaviorFromOpts(mapOptions)
    val dateRangeOpt = getDateRangeFromOpts(mapOptions)

    (readerConfigOpt, behaviorOpt, dateRangeOpt) match {
      case (Right(readerConfig), Right(behavior), Right(codMonthRange)) =>
        Right(AppConfig(readerConfig, behavior, codMonthRange))
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
}