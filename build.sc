import mill._
import mill.modules._
import scalalib._
import ammonite.ops._
import coursier.maven.MavenRepository
import mill.scalalib.{PublishModule, ScalaModule}
import mill.scalalib.publish.{Developer, License, PomSettings, VersionControl}
import $ivy.`com.lihaoyi::mill-contrib-versionfile:$MILL_VERSION`
import mill.contrib.versionfile.VersionFileModule

object versionFile extends VersionFileModule

object VersionOf {
  val `better-monadic-for` = "0.3.1"
  val cats                 = "2.3.1"
  val circe                = "0.12.3"
  val config               = "1.4.1"
  val `kind-projector`     = "0.11.3"
  val logback              = "1.2.3"
  val mantis               = "3.2.1-SNAPSHOT"
  val monix                = "3.3.0"
  val prometheus           = "0.10.0"
  val rocksdb              = "6.15.2"
  val scalacheck           = "1.15.2"
  val scalatest            = "3.2.5"
  val scalanet             = "0.8.0"
  val shapeless            = "2.3.3"
  val slf4j                = "1.7.30"
  val `scodec-core`        = "1.11.7"
  val `scodec-bits`        = "1.1.12"
  val scopt                = "4.0.1"
}

// Using 2.12.13 instead of 2.12.10 to access @nowarn, to disable certain deprecation
// warnings that come up in 2.13 but are too awkward to work around.
object metronome extends Cross[MetronomeModule]("2.12.13", "2.13.4")

class MetronomeModule(val crossScalaVersion: String) extends CrossScalaModule {

  // Get rid of the `metronome-2.13.4-` part from the artifact name. The JAR name suffix will shows the Scala version.
  // Check with `mill show metronome[2.13.4].__.artifactName` or `mill __.publishLocal`.
  private def removeCrossVersion(artifactName: String): String =
    "metronome-" + artifactName.split("-").drop(2).mkString("-")

  // In objects inheriting this trait, use `override def moduleDeps: Seq[PublishModule]`
  // to point at other modules that also get published. In other cases such as tests
  // it can be `override def moduleDeps: Seq[JavaModule]`, i.e. point at any module.
  trait Publishing extends PublishModule {
    def description: String

    // Make sure there's no newline in the file.
    override def publishVersion = versionFile.currentVersion().toString

    override def pomSettings = PomSettings(
      description = description,
      organization = "io.iohk",
      url = "https://github.com/input-output-hk/metronome",
      licenses = Seq(License.`Apache-2.0`),
      versionControl = VersionControl.github("input-output-hk", "metronome"),
      // Add yourself if you make a PR!
      // format: off
      developers = Seq(
        Developer("aakoshh", "Akosh Farkash", "https://github.com/aakoshh"),
        Developer("lemastero","Piotr Paradzinski","https://github.com/lemastero"),
        Developer("KonradStaniec","Konrad Staniec","https://github.com/KonradStaniec"),
        Developer("rtkaczyk", "Radek Tkaczyk", "https://github.com/rtkaczyk"),
        Developer("biandratti", "Maxi Biandratti", "https://github.com/biandratti"),
        Developer("dmitry-worker", "Dmitry Voronov", "https://github.com/dmitry-worker")
      )
      // format: on
    )
  }

  /** Common properties for all Scala modules. */
  trait SubModule extends ScalaModule {
    override def scalaVersion = crossScalaVersion
    override def artifactName = removeCrossVersion(super.artifactName())

    override def ivyDeps = Agg(
      ivy"org.typelevel::cats-core:${VersionOf.cats}",
      ivy"org.typelevel::cats-effect:${VersionOf.cats}"
    )

    override def scalacPluginIvyDeps = Agg(
      ivy"com.olegpy::better-monadic-for:${VersionOf.`better-monadic-for`}"
    )

    override def repositories = super.repositories ++ Seq(
      MavenRepository("https://oss.sonatype.org/content/repositories/snapshots")
    )

    override def scalacOptions = Seq(
      "-unchecked",
      "-deprecation",
      "-feature",
      "-encoding",
      "utf-8",
      "-Xfatal-warnings",
      "-Ywarn-value-discard"
    ) ++ {
      crossScalaVersion.take(4) match {
        case "2.12" =>
          // These options don't work well with 2.13
          Seq(
            "-Xlint:unsound-match",
            "-Ywarn-inaccessible",
            "-Ywarn-unused-import",
            "-Ywarn-unused:locals",
            "-Ywarn-unused:patvars",
            "-Ypartial-unification", // Required for the `>>` syntax.
            "-language:higherKinds",
            "-language:postfixOps"
          )
        case "2.13" =>
          Seq()
      }
    }

    // `extends Tests` uses the context of the module in which it's defined
    trait TestModule extends Tests {
      override def artifactName =
        removeCrossVersion(super.artifactName())

      override def scalacOptions =
        SubModule.this.scalacOptions

      // Test modules might depend on each other for example to get `Arbitrary` instances.
      def testModuleDeps: Seq[JavaModule] = Seq.empty

      override def moduleDeps: Seq[JavaModule] =
        super.moduleDeps ++ testModuleDeps
    }

    // Since mill 0.9.7 there can be only one `testFramework` per module.
    trait SpecsModule extends TestModule {
      override def testFramework = "org.scalatest.tools.Framework"

      // It may be useful to see logs in tests.
      override def moduleDeps: Seq[JavaModule] =
        super.moduleDeps ++ Seq(logging)

      // Enable logging in tests with `LogTracer`.
      // Control the visibility by creating a `./test/resources/logback-test.xml` file.
      // Alternatively, capture logs in memory with `InMemoryLogTracer`.
      override def ivyDeps = Agg(
        ivy"org.scalatest::scalatest:${VersionOf.scalatest}",
        ivy"ch.qos.logback:logback-classic:${VersionOf.logback}"
      )

      def single(args: String*) = T.command {
        // ScalaTest test
        super.runMain("org.scalatest.run", args: _*)
      }
    }

    trait PropsModule extends TestModule {
      override def testFramework = "org.scalacheck.ScalaCheckFramework"

      override def ivyDeps = Agg(
        ivy"org.scalacheck::scalacheck:${VersionOf.scalacheck}"
      )

      def single(args: String*) = T.command {
        // ScalaCheck test. Increased verbosity for longer stack traces.
        super.runMain(args.head, (args.tail ++ Seq("-verbosity", "3")): _*)
      }
    }
  }

  /** Abstractions shared between all modules. */
  object core extends SubModule with Publishing {
    override val description =
      "Common abstractions."

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"com.chuusai::shapeless:${VersionOf.shapeless}",
      ivy"io.monix::monix:${VersionOf.monix}"
    )

    object specs extends SpecsModule
  }

  /** Storage abstractions, e.g. a generic key-value store. */
  object storage extends SubModule {
    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"org.typelevel::cats-free:${VersionOf.cats}",
      ivy"org.scodec::scodec-bits:${VersionOf.`scodec-bits`}",
      ivy"org.scodec::scodec-core:${VersionOf.`scodec-core`}"
    )

    object specs extends SpecsModule
  }

  /** Emit trace events, abstracting away logs and metrics.
    *
    * Based on https://github.com/input-output-hk/iohk-monitoring-framework/tree/master/contra-tracer
    */
  object tracing extends SubModule with Publishing {
    override val description =
      "Abstractions for contravariant tracing."

    def scalacPluginIvyDeps = Agg(
      ivy"org.typelevel:::kind-projector:${VersionOf.`kind-projector`}"
    )
  }

  /** Additional crypto utilities such as threshold signature. */
  object crypto extends SubModule with Publishing {
    override val description =
      "Cryptographic primitives to support HotStuff and BFT proof verification."

    override def moduleDeps: Seq[PublishModule] =
      Seq(core)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"io.iohk::mantis-crypto:${VersionOf.mantis}",
      ivy"org.scodec::scodec-bits:${VersionOf.`scodec-bits`}",
      ivy"org.scodec::scodec-core:${VersionOf.`scodec-core`}"
    )

    object specs extends SpecsModule
  }

  /** Generic Peer-to-Peer components that can multiplex protocols
    * from different modules over a single authenticated TLS connection.
    */
  object networking extends SubModule with Publishing {
    override val description =
      "Generic networking library for keeping connections alive between federation members."

    override def moduleDeps: Seq[PublishModule] =
      Seq(tracing, crypto)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"io.iohk::scalanet:${VersionOf.scalanet}"
    )

    object specs extends SpecsModule
  }

  /** General configuration parser, to be used by application modules. */
  object config extends SubModule with Publishing {
    override val description = "Typesafe config wrapper powered by circe"

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"com.typesafe:config:${VersionOf.config}",
      ivy"io.circe::circe-core:${VersionOf.circe}",
      ivy"io.circe::circe-parser:${VersionOf.circe}",
      ivy"io.circe::circe-generic:${VersionOf.circe}"
    )

    object specs extends SpecsModule {
      override def ivyDeps = super.ivyDeps() ++ Agg(
        ivy"io.circe::circe-generic:${VersionOf.circe}"
      )
    }
  }

  /** Generic HotStuff BFT library. */
  object hotstuff extends SubModule {

    /** Pure consensus models. */
    object consensus extends SubModule with Publishing {
      override val description =
        "Pure HotStuff consensus models."

      override def moduleDeps: Seq[PublishModule] =
        Seq(core, crypto)

      object props extends PropsModule
      object specs extends SpecsModule
    }

    /** Expose forensics events via tracing. */
    object forensics extends SubModule

    /** Implements peer-to-peer communication, state and block synchronisation.
      *
      * Includes the remote communication protocol messages and networking.
      */
    object service extends SubModule {
      override def moduleDeps: Seq[JavaModule] =
        Seq(
          storage,
          tracing,
          crypto,
          networking,
          hotstuff.consensus,
          hotstuff.forensics
        )

      object props extends PropsModule {
        override def testModuleDeps = Seq(hotstuff.consensus.props)
      }
      object specs extends SpecsModule
    }
  }

  /** Components realising the checkpointing functionality using HotStuff. */
  object checkpointing extends SubModule {

    /** Library to be included on the PoW side to validate checkpoint certificats.
      *
      * Includes the certificate model and the checkpoint ledger and chain models.
      */
    object models extends SubModule with Publishing {
      override val description =
        "Checkpointing domain models, including the checkpoint certificate and its validation logic."

      override def ivyDeps = super.ivyDeps() ++ Agg(
        ivy"io.iohk::mantis-rlp:${VersionOf.mantis}"
      )

      override def moduleDeps: Seq[PublishModule] =
        Seq(core, crypto, hotstuff.consensus)

      object props extends PropsModule {
        override def testModuleDeps = Seq(hotstuff.consensus.props)
      }
      object specs extends SpecsModule {
        override def testModuleDeps = Seq(checkpointing.models.props)
      }
    }

    /** Library to be included on the PoW side to talk to the checkpointing service.
      *
      * Includes the local communication protocol messages and networking.
      */
    object interpreter extends SubModule with Publishing {
      override val description =
        "Components to implement a PoW side checkpointing interpreter."

      override def ivyDeps = Agg(
        ivy"io.iohk::scalanet:${VersionOf.scalanet}"
      )

      override def moduleDeps: Seq[PublishModule] =
        Seq(tracing, crypto, networking, checkpointing.models)

      object props extends PropsModule {
        override def testModuleDeps = Seq(checkpointing.models.props)
      }
      object specs extends SpecsModule
    }

    /** Implements the checkpointing functionality, validation rules,
      * state synchronisation, anything that is not an inherent part of
      * HotStuff, but applies to the checkpointing use case.
      *
      * If it was published, it could be directly included in the checkpoint
      * assisted blockchain application,  so the service and the interpreter
      * can share data in memory.
      */
    object service extends SubModule {
      override def moduleDeps: Seq[JavaModule] =
        Seq(
          tracing,
          storage,
          hotstuff.service,
          checkpointing.models,
          checkpointing.interpreter
        )

      object props extends PropsModule {
        override def moduleDeps: Seq[JavaModule] =
          super.moduleDeps ++ Seq(
            checkpointing.models.props,
            hotstuff.service.props
          )
      }
      object specs extends SpecsModule {
        override def testModuleDeps = Seq(checkpointing.models.props)
      }
    }

    /** Executable application for running HotStuff and checkpointing as a stand-alone process,
      * communicating with the interpreter over TCP.
      */
    object app extends SubModule {
      override def moduleDeps: Seq[JavaModule] =
        Seq(
          hotstuff.service,
          checkpointing.service,
          rocksdb,
          logging,
          metrics,
          config
        )

      override def ivyDeps = super.ivyDeps() ++ Agg(
        ivy"ch.qos.logback:logback-classic:${VersionOf.logback}",
        ivy"io.iohk::scalanet-discovery:${VersionOf.scalanet}",
        ivy"com.github.scopt::scopt:${VersionOf.scopt}"
      )
    }
  }

  /** Implements tracing abstractions to do structured logging.
    *
    * To actually emit logs, a dependant module also has to add
    * a dependency on e.g. logback.
    */
  object logging extends SubModule {
    override def moduleDeps: Seq[JavaModule] =
      Seq(tracing)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"org.slf4j:slf4j-api:${VersionOf.slf4j}",
      ivy"io.circe::circe-core:${VersionOf.circe}"
    )
  }

  /** Implements tracing abstractions to expose metrics to Prometheus. */
  object metrics extends SubModule {
    override def moduleDeps: Seq[JavaModule] =
      Seq(tracing)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"io.prometheus:simpleclient:${VersionOf.prometheus}",
      ivy"io.prometheus:simpleclient_httpserver:${VersionOf.prometheus}"
    )
  }

  /** Implements the storage abstractions using RocksDB. */
  object rocksdb extends SubModule {
    override def moduleDeps: Seq[JavaModule] =
      Seq(storage)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"org.rocksdb:rocksdbjni:${VersionOf.rocksdb}"
    )

    object props extends PropsModule {
      override def ivyDeps = super.ivyDeps() ++ Agg(
        ivy"io.monix::monix:${VersionOf.monix}"
      )
    }
  }

  /** Demo applications over example agreements. */
  object examples extends SubModule {
    override def moduleDeps: Seq[JavaModule] =
      Seq(
        hotstuff.service,
        config,
        logging,
        networking,
        rocksdb
      )

    override def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"com.github.scopt::scopt:${VersionOf.scopt}",
      ivy"ch.qos.logback:logback-classic:${VersionOf.logback}"
    )

    /** Entry point for the robot example. */
    def robot(args: String*) = T.command {
      super.runMain("io.iohk.metronome.examples.robot.app.RobotApp", args: _*)
    }

    object specs extends SpecsModule
  }
}
