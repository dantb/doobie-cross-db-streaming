package example

import java.time.Instant

import cats.effect.{ExitCode, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import doobie.free.connection
import doobie.implicits._
import doobie.postgres.implicits.pgEnum
import doobie.{ConnectionIO, Fragment, Meta}
import fs2.Stream
import Model._
import doobie.hikari.HikariTransactor

object MigrationProgram extends StrictLogging {

  def create(
      entitlementsTransactor: HikariTransactor[IO],
      zebraTransactor: HikariTransactor[IO]
  ): IO[ExitCode] = {
    def customerEntitlementsStream: Stream[ConnectionIO, StripeCustomerEntitlement] =
      Queries.abominationSelectQuery
        .query[StripeCustomerEntitlement]
        .stream

    def computeStateFromRecord(
        now: Instant,
        record: StripeCustomerEntitlement
    ): CustomerState.Value =
      record.maybeExpiry
        .map { expiry: Instant =>
          if (expiry.isAfter(now)) CustomerState.subscribed
          else CustomerState.cancelled
        }
        .getOrElse(CustomerState.unsubscribed) // no corresponding entitlement record, they have never been subscribed (edge case)

    def program: IO[ExitCode] = {
      logger.info("Running entitlement / zebra program script")

      val now = Instant.now()

      val computeState: StripeCustomerEntitlement => CustomerWithState =
        cust => CustomerWithState(cust, computeStateFromRecord(now, cust))

      val zebraSink: CustomerWithState => ConnectionIO[Int] =
        customerWithState => {
          Queries
            .insertIntoZebraQuery(customerWithState)
            .update
            .run
            .flatTap[Unit] { _ =>
              connection.delay {
                logger.info(
                  s"\nInserting record into zebra stripe_customer table: $customerWithState")
              }
            }
        }

      val stream: IO[Int] =
        CrossDatabaseStreaming
          .streamAcrossTransactors[StripeCustomerEntitlement, CustomerWithState, Int](
            source = customerEntitlementsStream.take(5), // use take for testing
            sink = zebraSink,
            transformSourceInput = computeState,
            sourceTransactor = entitlementsTransactor,
            sinkTransactor = zebraTransactor
          )

      stream
        .flatTap { inserted =>
          IO(
            logger.info(
              s"Inserted $inserted records into zebra stripe_customer table"))
        }
        .as(ExitCode.Success)
    }

    program
  }
}
