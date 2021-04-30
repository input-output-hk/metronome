package io.iohk.metronome.config

import cats.implicits._
import com.typesafe.config.{ConfigObject, ConfigRenderOptions}
import io.circe.{Json, JsonObject, ParsingFailure}
import io.circe.parser.parse

object ConfigParser {
  type ParsingResult = Either[ParsingFailure, Json]

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

  /** Turn `camelCaseKey` into `SNAKE_CASE_KEY`,
    * which is what it would look like as an env var.
    */
  def toSnakeCase(camelCase: String): String = {
    def loop(cs: List[Char], acc: List[Char]): String =
      cs match {
        case a :: b :: cs if a.isLower && b.isUpper =>
          loop(cs, b :: '_' :: a.toUpper :: acc)
        case '-' :: cs =>
          loop(cs, '_' :: acc)
        case a :: cs =>
          loop(cs, a.toUpper :: acc)
        case Nil =>
          acc.reverse.mkString
      }
    loop(camelCase.toList, Nil)
  }

  /** Transform all keys into camelCase form,
    * so they can be matched to case class fields.
    */
  def withCamelCase(json: Json): Json = {
    json
      .mapArray { arr =>
        arr.map(withCamelCase)
      }
      .mapObject { obj =>
        JsonObject(obj.toIterable.map { case (key, value) =>
          toCamelCase(key) -> withCamelCase(value)
        }.toList: _*)
      }
  }

  /** Apply overrides from the environment to a JSON structure.
    *
    * Only considers env var keys that start with prefix and are
    * in a PREFIX_SNAKE_CASE format.
    */
  def withEnvVarOverrides(
      json: Json,
      prefix: String,
      env: Map[String, String] = sys.env
  ): ParsingResult = {
    def extend(path: String, key: String) =
      if (path.isEmpty) key else s"${path}_${key}"

    def loop(json: Json, path: String): ParsingResult = {

      def tryParse(
          default: => Json,
          validate: Json => Boolean
      ): ParsingResult =
        env
          .get(path)
          .map { value =>
            val maybeJson = parse(value) orElse parse(s""""$value"""")

            maybeJson.flatMap { json =>
              if (validate(json)) {
                Right(json)
              } else {
                val msg = s"Invalid value for $path: $value"
                Left(ParsingFailure(value, new IllegalArgumentException(msg)))
              }
            }
          }
          .getOrElse(Right(default))

      json
        .fold[ParsingResult](
          jsonNull = tryParse(Json.Null, _ => true),
          jsonBoolean = x => tryParse(Json.fromBoolean(x), _.isBoolean),
          jsonNumber = x => tryParse(Json.fromJsonNumber(x), _.isNumber),
          jsonString = x => tryParse(Json.fromString(x), _.isString),
          jsonArray = { arr =>
            arr.zipWithIndex
              .map { case (value, idx) =>
                loop(value, extend(path, idx.toString))
              }
              .sequence
              .map { values =>
                Json.arr(values: _*)
              }
          },
          jsonObject = { obj =>
            obj.toIterable
              .map { case (key, value) =>
                val snakeKey = toSnakeCase(key)
                loop(value, extend(path, snakeKey)).map(key ->)
              }
              .toList
              .sequence
              .map { values =>
                Json.obj(values: _*)
              }
          }
        )
    }

    loop(json, prefix)
  }

}
