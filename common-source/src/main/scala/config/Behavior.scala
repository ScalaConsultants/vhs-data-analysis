package config

sealed trait Behavior {
  def label: String
}

case object Daily extends Behavior {
  def label: String = "daily"
}
case object Monthly extends Behavior {
  def label: String = "monthly"
}
case object Both extends Behavior {
  def label: String = "both"
}

object Behavior {

  /*
  app behavior can be daily, monthly or maybe both(both only works for enriched module)
  appBehavior by default is daily if it is not provided
  */
  def getBehaviorFromOpts(mapOpts: Map[String, String]): Either[String, Behavior] = {
    val behaviorOpt = mapOpts.get("behavior")
    behaviorOpt match {
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Daily.label) => Right(Daily)
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Monthly.label) => Right(Monthly)
      case Some(behaviorOpt) if behaviorOpt.equalsIgnoreCase(Both.label) => Right(Both)
      case None => Right(Daily)
      case _ => Left("app behavior introduced doesn't exist")
    }
  }
}