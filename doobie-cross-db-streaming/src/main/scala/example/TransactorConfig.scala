package example

import cats.effect.{ContextShift, IO}
import doobie.hikari.HikariTransactor

import scala.concurrent.ExecutionContext.Implicits.global

object TransactorConfig {

  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(global)

  def transactor(url: String, user: String, password: String): HikariTransactor[IO] =
    HikariTransactor
      .newHikariTransactor[IO](
        driverClassName = "org.postgresql.Driver",
        url = url,
        user = user,
        pass = password,
        connectEC = global,
        transactEC = global
      )
      .allocated
      .unsafeRunSync()
      ._1
}
