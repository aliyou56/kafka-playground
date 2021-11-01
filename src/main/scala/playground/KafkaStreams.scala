package playground

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{ GlobalKTable, JoinWindows }
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{ KafkaStreams, StreamsBuilder, StreamsConfig }

object KafkaStreamsPlayground {

  object Domain {
    type UserId  = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status  = String

    case class Order(
        orderId: OrderId,
        userId: UserId,
        products: List[Product],
        amount: BigInt,
      )
    case class Discount(profile: Profile, amount: BigInt)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    val OrdersByUser           = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts              = "discounts"
    val Orders                 = "orders"
    val Payments               = "payments"
    val PaidOrders             = "paid-orders"
  }

  import Domain._
  import Topics._

  implicit def serde[A >: Null: Encoder: Decoder]: Serde[A] = {
    val serializer   = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => decode[A](new String(bytes)).toOption

    Serdes.fromFn[A](serializer, deserializer)
  }

  // topology
  val builder = new StreamsBuilder()

  // KStream
  val userOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser)

  // KTable - is distributed
  val userProfilesTable: KTable[UserId, Profile] =
    builder.table[UserId, Profile](DiscountProfilesByUser)

  // GlobalKTable - copied to all nodes
  val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

  // KStream transformations: filter, map, mapValues, flatMap, flatMapValues
  val expensiveOrders: KStream[UserId, Order] =
    userOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

  val listOfProducts: KStream[UserId, List[Product]] =
    userOrdersStream.mapValues(order => order.products)

  val productsStream: KStream[UserId, Product] =
    userOrdersStream.flatMapValues(_.products)

  // join
  val ordersWithUserProfiles =
    userOrdersStream.join(userProfilesTable) { (order, profile) =>
      order -> profile
    }

  val discountedOrderStream =
    ordersWithUserProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) => profile }, // key of the join - picked from the 'left' stream
      {
        case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount)
      }, // values of hte matched records
    )

  // pick another identifier
  val ordersStream = discountedOrderStream.selectKey { (userId, order) =>
    order.orderId
  }
  val paymentsStream = builder.stream[OrderId, Payment](Payments)

  val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
  val joinOrdersPayments =
    (o: Order, p: Payment) => if (p.status == "PAID") Option(o) else Option.empty[Order]
  val ordersPaid =
    ordersStream
      .join(paymentsStream)(joinOrdersPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toIterable)

  // sink
  ordersPaid.to(PaidOrders)

  val topology = builder.build()

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    // println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()

    /*List(
      "orders-by-user",
      "discount-profiles-by-user",
      "discounts",
      "orders",
      "payments",
      "paid-orders",
    ).foreach { topic =>
      println(s"kafka-topics --bootstrap-server localhost:9092 --topic $topic --create")
    }*/

    /*
      $KAFKA_HOME/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic paid-orders


      $KAFKA_HOME/bin/kafka-console-producer.sh \
        --topic discounts \
        --broker-list localhost:9092 \
        --property parse.key=true \
        --property key.separator=,

      profile1,{ "profile":"profile1","amount":10 }
      profile2,{ "profile":"profile2","amount":100 }
      profile3,{ "profile":"profile3","amount":40 }

      $KAFKA_HOME/bin/kafka-console-producer.sh \
        --topic discount-profiles-by-user \
        --broker-list localhost:9092 \
        --property parse.key=true \
        --property key.separator=,

      Daniel,profile1
      Ricardo,profile2
      Aliyou,profile3


      $KAFKA_HOME/bin/kafka-console-producer.sh \
        --topic orders-by-user \
        --broker-list localhost:9092 \
        --property parse.key=true \
        --property key.separator=,

      Daniel,{ "orderId":"order1", "user":"Daniel", "products":["iPhone 13","MacBook Pro 15"],"amount":4000.0 }
      Ricardo,{ "orderId":"order2", "user":"Ricardo", "products":["iPhone 11"],"amount":800.0 }


      $KAFKA_HOME/bin/kafka-console-producer.sh \
        --topic payments \
        --broker-list localhost:9092 \
        --property parse.key=true \
        --property key.separator=,

      order1,{"orderId":"order1","status":"PAID"}
      order2,{"orderId":"order2","status":"PENDING"}

     */
  }
}
