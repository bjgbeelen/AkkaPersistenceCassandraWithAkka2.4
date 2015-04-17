package demo

import akka.actor.Actor
import akka.actor.Props

object ReceiverActor {
  case object AddOne
  case object Done
}

class ReceiverActor extends Actor {

  import ReceiverActor._

  def receive = {
    case AddOne => {
      println("one added")
      sender() ! Done
    }
  }

}
