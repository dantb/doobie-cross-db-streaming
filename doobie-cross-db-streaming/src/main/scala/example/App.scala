package example

import cats.syntax.flatMap._
import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.scalalogging.StrictLogging

object App extends IOApp with StrictLogging {
  override def run(args: List[String]): IO[ExitCode] = {

    val (entUrl: String,
         entUser: String,
         entPass: String,
         zebraUrl: String,
         zebraUser: String,
         zebraPass: String) =
      (args(0), args(1), args(2), args(3), args(4), args(5))

    val entitlementsDatabase = TransactorConfig.transactor(entUrl, entUser, entPass)
    val zebraDatabase        = TransactorConfig.transactor(zebraUrl, zebraUser, zebraPass)

    val program = MigrationProgram.create(entitlementsDatabase, zebraDatabase)

    program.flatTap { _ =>
      IO(logger.info("Entitlement / zebra migration program complete"))
    }
  }
}
