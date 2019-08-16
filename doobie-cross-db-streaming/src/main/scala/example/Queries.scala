package example

import doobie._
import doobie.implicits._
import example.Model._

object Queries {
  val abominationSelectQuery: Fragment =
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
}
