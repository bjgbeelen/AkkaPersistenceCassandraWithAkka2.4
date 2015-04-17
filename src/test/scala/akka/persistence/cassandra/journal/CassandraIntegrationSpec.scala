package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraIntegrationSpec extends PluginSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |akka.test.single-expect-default = 10s
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = 9142
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)
  val system = ActorSystem("test", config)

  def getSnapshotStore: ActorRef = extension.snapshotStoreFor(null)

  case class DeleteTo(snr: Long, permanent: Boolean)

  class PersistentActorA(val persistenceId: String) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr, permanent) =>
        deleteMessages(sequenceNr, permanent)
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        sender ! payload
        sender ! lastSequenceNr
        sender ! recoveryRunning
    }

    override private[persistence] lazy val snapshotStore: ActorRef = getSnapshotStore
  }

  class PersistentActorC(val persistenceId: String, val probe: ActorRef) extends PersistentActor {
    var last: String = _

    def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case payload: String =>
        handle(payload)
    }

    def receiveCommand: Receive = {
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case payload: String =>
        persist(payload)(handle)

    }

    def handle: Receive = {
      case payload: String =>
        last = s"${payload}-${lastSequenceNr}"
        probe ! s"updated-${last}"
    }

    override private[persistence] lazy val snapshotStore: ActorRef = getSnapshotStore
  }

  class PersistentActorCNoRecover(override val persistenceId: String, probe: ActorRef) extends PersistentActorC(persistenceId, probe) {
    override def preStart() = ()

    override private[persistence] lazy val snapshotStore: ActorRef = getSnapshotStore
  }

  class ViewA(val viewId: String, val persistenceId: String, probe: ActorRef) extends PersistentView {
    def receive = {
      case payload =>
        probe ! payload
    }

    override def autoUpdate: Boolean = false

    override def autoUpdateReplayMax: Long = 0
  }
}

import CassandraIntegrationSpec._

class CassandraIntegrationSpec extends TestKit(CassandraIntegrationSpec.system) with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  def subscribeToConfirmation(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[Delivered])

  def subscribeToBatchDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitBatchDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testIndividualDelete(persistenceId: String, permanent: Boolean): Unit = {
    val deleteProbe = TestProbe()
    subscribeToBatchDeletion(deleteProbe)

    val persistentActor1 = system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    1L to 16L foreach { i =>
      persistentActor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    // delete single message in partition
    persistentActor1 ! DeleteTo(12L, permanent)
    awaitBatchDeletion(deleteProbe)

    system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    1L to 16L foreach { i =>
      if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
    }

    // delete whole partition
    6L to 10L foreach { i =>
      persistentActor1 ! DeleteTo(i, permanent)
      awaitBatchDeletion(deleteProbe)
    }

    system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    1L to 5L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
    11L to 16L foreach { i =>
      if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  def testRangeDelete(persistenceId: String, permanent: Boolean): Unit = {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val persistentActor1 = system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    1L to 16L foreach { i =>
      persistentActor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    persistentActor1 ! DeleteTo(3L, permanent)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    4L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    persistentActor1 ! DeleteTo(7L, permanent)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[PersistentActorA], persistenceId))
    8L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A Cassandra journal" should {
    "write and replay messages" in {
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorA], "p1"))
      1L to 16L foreach { i =>
        persistentActor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorA], "p1"))
      1L to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      persistentActor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }
    // "not replay messages marked as deleted" in {
    //   testIndividualDelete("p3", false)
    // }
    // "not replay permanently deleted messages" in {
    //   testIndividualDelete("p4", true)
    // }
    "not replay messages marked as range-deleted" in {
      testRangeDelete("p5", false)
    }
    "not replay permanently range-deleted messages" in {
      testRangeDelete("p6", true)
    }
    "replay messages incrementally" in {
      val probe = TestProbe()
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorA], "p7"))
      1L to 6L foreach { i =>
        persistentActor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val view = system.actorOf(Props(classOf[ViewA], "p7-view", "p7", probe.ref))
      probe.expectNoMsg(200.millis)

      view ! Update(true, replayMax = 3L)
      probe.expectMsg(s"a-1")
      probe.expectMsg(s"a-2")
      probe.expectMsg(s"a-3")
      probe.expectNoMsg(200.millis)

      view ! Update(true, replayMax = 3L)
      probe.expectMsg(s"a-4")
      probe.expectMsg(s"a-5")
      probe.expectMsg(s"a-6")
      probe.expectNoMsg(200.millis)
    }
  }

  "A persistentActor" should {
    "recover from a snapshot with follow-up messages" in {
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorC], "p10", testActor))
      persistentActor1 ! "a"
      expectMsg("updated-a-1")
      persistentActor1 ! "snap"
      expectMsg("snapped-a-1")
      persistentActor1 ! "b"
      expectMsg("updated-b-2")

      system.actorOf(Props(classOf[PersistentActorC], "p10", testActor))
      expectMsg("offered-a-1")
      expectMsg("updated-b-2")
    }
    "recover from a snapshot with follow-up messages and an upper bound" in {
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorCNoRecover], "p11", testActor))
      persistentActor1 ! Recover()
      persistentActor1 ! "a"
      expectMsg("updated-a-1")
      persistentActor1 ! "snap"
      expectMsg("snapped-a-1")
      2L to 7L foreach { i =>
        persistentActor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorCNoRecover], "p11", testActor))
      persistentActor2 ! Recover(toSequenceNr = 3L)
      expectMsg("offered-a-1")
      expectMsg("updated-a-2")
      expectMsg("updated-a-3")
      persistentActor2 ! "d"
      expectMsg("updated-d-8")
    }
    "recover from a snapshot without follow-up messages inside a partition" in {
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorC], "p12", testActor))
      persistentActor1 ! "a"
      expectMsg("updated-a-1")
      persistentActor1 ! "snap"
      expectMsg("snapped-a-1")

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorC], "p12", testActor))
      expectMsg("offered-a-1")
      persistentActor2 ! "b"
      expectMsg("updated-b-2")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition is invalid)" in {
      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorC], "p13", testActor))
      1L to 5L foreach { i =>
        persistentActor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      persistentActor1 ! "snap"
      expectMsg("snapped-a-5")

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorC], "p13", testActor))
      expectMsg("offered-a-5")
      persistentActor2 ! "b"
      expectMsg("updated-b-6")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a message marked as deleted)" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorC], "p14", testActor))
      1L to 5L foreach { i =>
        persistentActor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      persistentActor1 ! "snap"
      expectMsg("snapped-a-5")

      persistentActor1 ! "a"
      expectMsg("updated-a-6")

      persistentActor1 ! DeleteTo(6L, false)
      awaitBatchDeletion(deleteProbe)

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorC], "p14", testActor))
      expectMsg("offered-a-5")
      persistentActor2 ! "b"
      expectMsg("updated-b-7")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a permanently deleted message)" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val persistentActor1 = system.actorOf(Props(classOf[PersistentActorC], "p15", testActor))
      1L to 5L foreach { i =>
        persistentActor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      persistentActor1 ! "snap"
      expectMsg("snapped-a-5")

      persistentActor1 ! "a"
      expectMsg("updated-a-6")

      persistentActor1 ! DeleteTo(6L, true)
      awaitBatchDeletion(deleteProbe)

      val persistentActor2 = system.actorOf(Props(classOf[PersistentActorC], "p15", testActor))
      expectMsg("offered-a-5")
      persistentActor2 ! "b"
      expectMsg("updated-b-6") // sequence number of permanently deleted message can be re-used
    }
    "properly recover after all messages have been deleted" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val p = system.actorOf(Props(classOf[PersistentActorA], "p16"))

      p ! "a"
      expectMsgAllOf("a", 1L, false)

      p ! DeleteTo(1L, true)
      awaitBatchDeletion(deleteProbe)

      val r = system.actorOf(Props(classOf[PersistentActorA], "p16"))

      r ! "b"
      expectMsgAllOf("b", 1L, false)
    }
  }
}
