package config

import scala.util.matching.Regex

sealed trait MethodAnalyzer

final case class ElbowAnalyzer(fromK: Int = 3, toK: Int = 25) extends MethodAnalyzer

object  ElbowAnalyzer {
  def methodLabel: String = "elbow"
}

final case class KMeansAnalyzer(k: Int) extends MethodAnalyzer

object KMeansAnalyzer {
  def methodLabel:String = "k-means"
}

object MethodAnalyzer {

  val kClusters: Regex = "([0-9]+)".r

  private def getElbowMethodFromOpts(mapOpts: Map[String, String]): Either[String, ElbowAnalyzer] = {
    val (fromDateOpt, toDateOpt) = (mapOpts.get("fromK"), mapOpts.get("toK"))
    (fromDateOpt, toDateOpt) match {
      case (Some(kClusters(fromK)), Some(kClusters(toK))) =>  Right(ElbowAnalyzer(Integer.parseInt(fromK), Integer.parseInt(toK)))
      case (None, None) => Right(ElbowAnalyzer())
      case _ => Left("invalid inputs for Elbow method")
    }
  }

  private def getKMeansMethodFromOpts(mapOpts: Map[String, String]): Either[String, KMeansAnalyzer] =
    mapOpts.get("k") match {
      case Some(kClusters(k)) =>  Right(KMeansAnalyzer(Integer.parseInt(k)))
      case _ => Left("invalid inputs for KMeans method")
    }

  def getAnalyzerMethodFromOpts(mapOpts: Map[String, String]): Either[String, MethodAnalyzer] = {
    mapOpts.get("method")  match {
      case Some(method) if method.equalsIgnoreCase(ElbowAnalyzer.methodLabel) =>  getElbowMethodFromOpts(mapOpts)
      case Some(method) if method.equalsIgnoreCase(KMeansAnalyzer.methodLabel) =>  getKMeansMethodFromOpts(mapOpts)
      case _ => Left("invalid method analyzer")
    }
  }

}