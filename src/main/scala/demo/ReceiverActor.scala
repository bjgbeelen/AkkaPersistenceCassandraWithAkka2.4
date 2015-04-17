package demo

import akka.actor._
import akka.persistence._
import scala.concurrent._
import duration._

object CounterActor {
  sealed trait Command
  case object AddOne extends Command
  case object Done extends Command

  sealed trait Event
  case object OneAdded extends Event
}

class CounterActor extends PersistentActor {

  import CounterActor._
  import context.dispatcher

  override val persistenceId = s"persistent-counter-actor"

  context.system.scheduler.schedule(2.seconds, 2.seconds, self, AddOne)

  var counter = 0;

  override def receiveCommand = {
    case AddOne => 
      persist(OneAdded) { e =>
        updateState(e)
        println(s"I'm now at $counter")
    }
  }

  override def receiveRecover: Receive = {
    case e: Event ⇒ updateState(e)
  }

  def updateState(e: Event) = e match {
    case OneAdded => counter += 1
  }

}
