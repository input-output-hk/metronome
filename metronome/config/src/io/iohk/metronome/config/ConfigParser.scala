package io.iohk.metronome.config

import cats.implicits._
import com.typesafe.config.{ConfigObject, ConfigRenderOptions, ConfigUtil}
import io.circe.{Decoder, DecodingFailure, Json, JsonObject, ParsingFailure}
import io.circe.parser.{parse => parseJson}

import scala.util.{Failure, Success, Try}

object ConfigParser {
  protected[config] type ParsingResult = Either[ParsingFailure, Json]

  type Result[T] = Either[Either[ParsingFailure, DecodingFailure], T]

  /** Parse configuration into a type using a JSON decoder, thus allowing
    * validations to be applied to all configuraton values up front, rather
    * than fail lazily when something is accessed or instantiated from
    * the config factory.
    *
    * Accept overrides from the environment in PREFIX_PATH_TO_FIELD format.
    */
  def parse[T: Decoder](
      conf: => ConfigObject,
      prefix: String = "",
      env: Map[String, String] = sys.env
  ): Result[T] =
    Try(conf) match {
      case Success(conf) =>
        // Render the whole config to JSON. Everything needs a default value,
        // but it can be `null` and be replaced from the environment.
        val orig = toJson(conf)
        // Transform fields which use dash for segmenting into camelCase.
        val withCamel = withCamelCase(orig)
        // Apply overrides from env vars.
        val withEnv = withEnvVarOverrides(withCamel, prefix, env)
        // Map to the domain config model.
        withEnv match {
          case Left(error) => Left(Left(error))
          case Right(json) =>
            Decoder[T].decodeJson(json) match {
              case Left(error)  => Left(Right(error))
              case Right(value) => Right(value)
            }
        }

      case Failure(ex) =>
        val msg = s"Could not load the config: ${ex.getMessage}"
        Left(Left(io.circe.ParsingFailure(msg, ex)))
    }

  /** Render a TypeSafe Config section into JSON. */
  protected[config] def toJson(conf: ConfigObject): Json = {
    val raw = conf.render(ConfigRenderOptions.concise)
    parseJson(raw) match {
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
  protected[config] def toCamelCase(key: String): String = {
    def loop(cs: List[Char]): List[Char] =
      cs match {
        case ('_' | '-') :: cs =>
          cs match {
            case c :: cs => c.toUpper :: loop(cs)
            case Nil     => Nil
          }
        case c :: cs => c :: loop(cs)
        case Nil     => Nil
      }

    loop(key.toList).mkString
  }

  /** Turn `camelCaseKey` into `SNAKE_CASE_KEY`,
    * which is what it would look like as an env var.
    */
  protected[config] def toSnakeCase(camelCase: String): String = {
    def loop(cs: List[Char]): List[Char] =
      cs match {
        case a :: b :: cs if a.isLower && b.isUpper =>
          a.toUpper :: '_' :: b :: loop(cs)
        case '-' :: cs =>
          '_' :: loop(cs)
        case a :: cs =>
          a.toUpper :: loop(cs)
        case Nil =>
          Nil
      }

    loop(camelCase.toList).mkString
  }

  /** Transform all keys into camelCase form,
    * so they can be matched to case class fields.
    */
  protected[config] def withCamelCase(json: Json): Json = {
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
    *
    * The operation can fail if a value in the environment is
    * incompatible with the default in the config files.
    *
    * Default values in the config file are necessary, because
    * the environment variable name in itself doesn't uniquely
    * define a data structure (a single underscore matches both
    * a '.' or a '-' in the path).
    */
  protected[config] def withEnvVarOverrides(
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
            val maybeJson =
              parseJson(value) orElse parseJson(ConfigUtil.quoteString(value))

            maybeJson.left
              .map { err =>
                val msg = s"Could not parse value for $path: $value"
                ParsingFailure(msg, err)
              }
              .flatMap { json =>
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
