package io.iohk.metronome.config

import io.circe.{Json, JsonObject, ParsingFailure}
import io.circe.parser.parse
import com.typesafe.config.{ConfigObject, ConfigRenderOptions}

object ConfigParser {

  /** Render a TypeSafe Config section into JSON. */
  def toJson(conf: ConfigObject): Json = {
    val raw = conf.render(ConfigRenderOptions.concise)
    parse(raw) match {
      case Left(error: ParsingFailure) =>
        // This shouldn't happen with a well formed config file,
        // which would have already failed during parsing or projecting
        // to a `ConfigObject` passed to this method.
        throw new IllegalArgumentException(error.message, error.underlying)

      case Right(json) =>
        json
    }
  }

  /** Transform all keys into camelCase form,
    * so they can be matched to case class fields.
    */
  def toCamelCase(json: Json): Json = {
    json
      .mapArray { arr =>
        arr.map(toCamelCase)
      }
      .mapObject { obj =>
        JsonObject(obj.toIterable.map { case (key, value) =>
          toCamelCase(key) -> toCamelCase(value)
        }.toList: _*)
      }
  }

  /** Transform a key in the HOCON config file to camelCase. */
  def toCamelCase(key: String): String = {
    def loop(cs: List[Char], acc: List[Char]): String =
      cs match {
        case ('_' | '-') :: cs =>
          cs match {
            case c :: cs => loop(cs, c.toUpper :: acc)
            case cs      => loop(cs, acc)
          }
        case c :: cs => loop(cs, c :: acc)
        case Nil     => acc.reverse.mkString
      }
    loop(key.toList, Nil)
  }

  /** Apply overrides from the environment to a JSON structure. */
  // def withEnvVarOverrides(json: Json, prefix: String): Json = {
  //   def loop(json: Json, path: ): Json =
  // }

  /** Turn `camelCaseKey` into `CAMEL_CASE_KEY`,
    * which is what it would look like as an env var.
    */
  // def snakeify(camelCase: String): String = ???
}
