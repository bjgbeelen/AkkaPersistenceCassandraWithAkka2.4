package demo

import akka.actor.Actor
import akka.actor.Props

class SenderActor extends Actor {

  override def preStart(): Unit = {
    val receiver = context.actorOf(Props[CounterActor], "receiver")
    receiver ! CounterActor.AddOne
  }

  def receive = {
    case CounterActor.Done => context.stop(self)
  }

}
