package example

import java.time.Instant

import doobie.postgres.implicits.pgEnum
import doobie.util.Meta

object Model {
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

    implicit val customerStateMeta: Meta[CustomerState.Value] =
      pgEnum(CustomerState, "customer_state")
  }

  final case class CustomerWithState(
    customer: StripeCustomerEntitlement,
    state: CustomerState.Value
  )
}
