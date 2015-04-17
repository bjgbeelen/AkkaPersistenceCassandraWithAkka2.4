package demo

import akka.actor._

object Main extends App {

  implicit val system = ActorSystem("demo")

  val sender = system.actorOf(Props[SenderActor], "sender")
  // val receiver = system.actorOf(Props[ReceiverActor], "receiver")

  println("Hello world")
}
