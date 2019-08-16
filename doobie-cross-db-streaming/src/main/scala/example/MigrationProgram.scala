package example

import java.time.Instant

import cats.implicits._
import cats.effect.{ContextShift, ExitCode, IO}
import cats.free.Free
import com.typesafe.scalalogging.StrictLogging
import doobie.free.connection
import doobie.free.connection.ConnectionOp
import doobie.{ConnectionIO, Fragment, Meta}
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.implicits.pgEnum
import fs2.Stream
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global

object MigrationProgram extends StrictLogging {
  def create(
      entUrl: String,
      entUser: String,
      entPass: String,
      zebraUrl: String,
      zebraUser: String,
      zebraPass: String
  ): IO[ExitCode] = {

    implicit val ioContextShift: ContextShift[IO] = IO.contextShift(global)
    val entitlementsDatabase = HikariTransactor
      .newHikariTransactor[IO]("org.postgresql.Driver",
                               entUrl,
                               entUser,
                               entPass,
                               global,
                               global)
      .allocated
      .unsafeRunSync()
      ._1
    val zebraDatabase = HikariTransactor
      .newHikariTransactor[IO]("org.postgresql.Driver",
                               zebraUrl,
                               zebraUser,
                               zebraPass,
                               global,
                               global)
      .allocated
      .unsafeRunSync()
      ._1

    final case class StripeCustomerEntitlement(
        itvId: String,
        stripeCustomerId: String,
        maybeStart: Option[Instant],
        maybeExpiry: Option[Instant],
        hasHadFreeTrial: Boolean,
        timestamp: Instant
    )

    object CustomerState extends Enumeration {
      val subscribed, unsubscribed, cancelled = Value
    }

    final case class CustomerWithState(
      customer: StripeCustomerEntitlement,
      state: CustomerState.Value
    )

    implicit val customerStateMeta: Meta[CustomerState.Value] =
      pgEnum(CustomerState, "customer_state")

    val abominationQuery: Fragment =
      sql"""SELECT mostRecentCustomer.itv_id,
           |       mostRecentCustomer.stripe_customer_id,
           |       mostRecentEntitlement.start,
           |       mostRecentEntitlement.expiry,
           |       mostRecentCustomer.has_had_free_trial,
           |       mostRecentCustomer.timestamp
           |FROM
           |-- most recent stripe customer id per itv id
           |((SELECT itv_id,
           |        stripe_customer_id,
           |        has_had_free_trial,
           |        timestamp
           | FROM   (SELECT itv_id,
           |                stripe_customer_id,
           |                has_had_free_trial,
           |                timestamp,
           |                Rank()
           |                  OVER (
           |                    partition BY itv_id
           |                    ORDER BY timestamp DESC)
           |         FROM stripe_customer) AS orderedCustomers
           | WHERE rank = 1) AS mostRecentCustomer
           | -- include stripe customer records for which no entitlements exist
           | LEFT OUTER JOIN
           | -- most recent entitlement per itv id
           | (SELECT itv_id,
           |         start,
           |         expiry
           |  FROM   (SELECT itv_id,
           |                 start,
           |                 expiry,
           |                 timestamp,
           |                 Rank()
           |                   OVER (
           |                     partition BY itv_id
           |                     ORDER BY timestamp DESC)
           |          FROM entitlement) AS orderedEntitlements
           |  WHERE rank = 1) AS mostRecentEntitlement
           |-- join on itv id
           |ON mostRecentCustomer.itv_id = mostRecentEntitlement.itv_id)
           |""".stripMargin

    def insertIntoZebraQuery(customerWithState: CustomerWithState): Fragment =
      sql"""INSERT INTO stripe_customer (user_id, stripe_id, time_created, has_had_free_trial, state, updated_at)
           |VALUES (${customerWithState.customer.itvId}, ${customerWithState.customer.stripeCustomerId},
           |${customerWithState.customer.timestamp}, ${customerWithState.customer.hasHadFreeTrial},
           |${customerWithState.state}, '1970-01-01 00:00:00')
           |ON CONFLICT (user_id) DO NOTHING
           |""".stripMargin

    def customerEntitlementsStream
      : Stream[ConnectionIO, StripeCustomerEntitlement] =
      abominationQuery
        .query[StripeCustomerEntitlement]
        .stream

    def computeStateFromRecord(now: Instant, record: StripeCustomerEntitlement): CustomerState.Value =
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

      val zebraSink: CustomerWithState => ConnectionIO[Int] = customerWithState => {
        insertIntoZebraQuery(customerWithState)
          .update
          .run
          .flatTap[Unit] { _ =>
            connection.delay {
              logger.info(s"\nInserting record into zebra stripe_customer table: $customerWithState")
            }
          }
      }

      val stream: IO[Int] = CrossDatabaseStreaming
        .streamAcrossTransactors[StripeCustomerEntitlement, CustomerWithState, Int](
          source = customerEntitlementsStream.take(5), // use take for testing
          sink = zebraSink,
          transformSourceInput = computeState,
          sourceTransactor = entitlementsDatabase,
          sinkTransactor = zebraDatabase
        )

      stream
        .flatTap { inserted =>
          IO(logger.info(s"Inserted $inserted records into zebra stripe_customer table"))
        }
        .as(ExitCode.Success)
    }

    program
  }
}
