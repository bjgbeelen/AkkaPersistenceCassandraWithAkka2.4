package demo

import language.postfixOps
import akka.actor._
import akka.persistence._
import akka.persistence.AtLeastOnceDelivery._
import scala.concurrent._
import duration._

object CounterActor {
  sealed trait Command
  case object AddOne extends Command
  case object Done extends Command

  sealed trait Event
  case object OneAdded extends Event

  private case class Snapshot(deliverySnapshot: AtLeastOnceDeliverySnapshot)
}

class CounterActor extends PersistentActor
  with AtLeastOnceDelivery {

  import CounterActor._
  import context.dispatcher

  override val persistenceId = s"persistent-counter-actor"

  context.system.scheduler.schedule(2 seconds, 2 seconds, self, AddOne)

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
    case SnapshotOffer(_, snapshot: Snapshot) => {
      println(s"Snapshot offered")
      setDeliverySnapshot(snapshot.deliverySnapshot)
    }
  }

  override def unhandled(msg: Any): Unit = msg match {
    case SaveSnapshotSuccess(_)        ⇒ println(s"Snapshot saved!")// ignore but prevent dead-letter warnings
    case SaveSnapshotFailure(_, cause) ⇒ println(s"Failed to save a snapshot. Cause: $cause")//log.warning("Failed to save a snapshot. Cause: " + cause)
    case _                             ⇒ super.unhandled(msg)
  }

  def updateState(e: Event) = e match {
    case OneAdded => {
      counter += 1
      if (counter % 10 == 0) {
        saveSnapshot(Snapshot(getDeliverySnapshot))
      }
    }
  }


}
