package demo

import akka.actor.Actor
import akka.actor.Props

class SenderActor extends Actor {

  override def preStart(): Unit = {
    val receiver = context.actorOf(Props[ReceiverActor], "receiver")
    receiver ! ReceiverActor.AddOne
  }

  def receive = {
    case ReceiverActor.Done => context.stop(self)
  }

}
