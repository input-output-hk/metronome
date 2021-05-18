package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Concurrent, Timer, Fiber, Resource, ContextShift}
import cats.effect.concurrent.Ref
import io.iohk.metronome.core.Validated
import io.iohk.metronome.core.fibers.FiberSet
import io.iohk.metronome.hotstuff.consensus.ViewNumber
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Effect,
  Event,
  ProtocolState,
  ProtocolError,
  Phase,
  Message,
  Block,
  Signing,
  QuorumCertificate
}
import io.iohk.metronome.hotstuff.service.execution.BlockExecutor
import io.iohk.metronome.hotstuff.service.pipes.SyncPipe
import io.iohk.metronome.hotstuff.service.storage.{
  BlockStorage,
  ViewStateStorage
}
import io.iohk.metronome.hotstuff.service.tracing.ConsensusTracers
import io.iohk.metronome.networking.{ConnectionHandler, Network}
import io.iohk.metronome.storage.KVStoreRunner
import monix.catnap.ConcurrentQueue
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.util.control.NonFatal
import io.iohk.metronome.hotstuff.service.execution.BlockExecutor

/** An effectful executor wrapping the pure HotStuff ProtocolState.
  *
  * It handles the `consensus.basic.Message` events coming from the network.
  */
class ConsensusService[
    F[_]: Timer: Concurrent,
    N,
    A <: Agreement: Block: Signing
](
    publicKey: A#PKey,
    network: Network[F, A#PKey, Message[A]],
    appService: ApplicationService[F, A],
    blockExecutor: BlockExecutor[F, N, A],
    blockStorage: BlockStorage[N, A],
    viewStateStorage: ViewStateStorage[N, A],
    stateRef: Ref[F, ProtocolState[A]],
    stashRef: Ref[F, ConsensusService.MessageStash[A]],
    counterRef: Ref[F, ConsensusService.MessageCounter],
    syncPipe: SyncPipe[F, A]#Left,
    eventQueue: ConcurrentQueue[F, Event[A]],
    fiberSet: FiberSet[F],
    maxEarlyViewNumberDiff: Int
)(implicit tracers: ConsensusTracers[F, A], storeRunner: KVStoreRunner[F, N]) {

  import ConsensusService.MessageCounter

  /** Get the current protocol state, perhaps to respond to status requests. */
  def getState: F[ProtocolState[A]] =
    stateRef.get

  /** Process incoming network messages. */
  private def processNetworkMessages: F[Unit] =
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        validateMessage(Event.MessageReceived(from, message)).flatMap {
          case None =>
            ().pure[F]
          case Some(valid) =>
            syncDependencies(valid)
        }
      }
      .completedL

  /** First round of validation of message to decide if we should process it at all. */
  private def validateMessage(
      event: Event.MessageReceived[A]
  ): F[Option[Validated[Event.MessageReceived[A]]]] =
    stateRef.get.flatMap { state =>
      state
        .validateMessage(event)
        .map(m => m: Event.MessageReceived[A]) match {
        case Left(error) =>
          protocolError(error).as(none)

        case Right(
              Event.MessageReceived(
                sender,
                message @ Message.Prepare(_, _, highQC)
              )
            ) if state.commitQC.viewNumber > highQC.viewNumber =>
          // The sender is building on a block that is older than the committed one.
          // This could be an attack, forcing us to re-download blocks we already pruned.
          protocolError(ProtocolError.UnsafeExtension[A](sender, message))
            .as(none)

        case Right(valid) if isFromPast(valid, state.viewNumber) =>
          tracers.fromPast(valid) >>
            counterRef.update(_.incPast).as(none)

        case Right(valid) if isFromFuture(valid, state.viewNumber) =>
          tracers.fromFuture(valid) >>
            counterRef.update(_.incFuture).as(none)

        case Right(valid) =>
          // We know that the message is to/from the leader and it's properly signed,
          // althought it may not match our current state, which we'll see later.
          counterRef.update(_.incPresent).as(validated(valid).some)
      }
    }

  private def isFromPast(
      e: Event.MessageReceived[A],
      viewNumber: ViewNumber
  ): Boolean =
    e.message match {
      case m: Message.NewView[_] => m.viewNumber < viewNumber.prev
      case m                     => m.viewNumber < viewNumber
    }

  private def isFromFuture(
      e: Event.MessageReceived[A],
      viewNumber: ViewNumber
  ): Boolean =
    e.message.viewNumber > viewNumber + maxEarlyViewNumberDiff

  /** Synchronize any missing block dependencies, then enqueue the event for final processing. */
  private def syncDependencies(
      message: Validated[Event.MessageReceived[A]]
  ): F[Unit] = {
    import Message._
    // Only syncing Prepare messages. They have the `highQC` as block parent,
    // so we know that is something that is safe to sync, it's not a DoS attack.
    // Other messages may be bogus:
    // - a Vote can point at a non-existing block to force some download;
    //   we'd reject it anyway if it doesn't match the state we prepared
    // - a Quorum could be a replay of some earlier one, maybe a block we have pruned
    // - a NewView is similar, it's best to first wait and select the highest we know
    message.message match {
      case prepare @ Prepare(_, block, highQC)
          if Block[A].parentBlockHash(block) != highQC.blockHash =>
        // The High Q.C. may be valid, but the block is not built on it.
        protocolError(ProtocolError.UnsafeExtension(message.sender, prepare))

      case prepare: Prepare[_] =>
        // Carry out syncing and validation asynchronously.
        syncAndValidatePrepare(message.sender, prepare)

      case _: Vote[_] =>
        // Let the ProtocolState reject it if it's not about the prepared block.
        enqueueEvent(message)

      case _: Quorum[_] =>
        // Let the ProtocolState reject it if it's not about the prepared block.
        enqueueEvent(message)

      case _: NewView[_] =>
        // Let's assume that we will have the highest prepare Q.C. available,
        // while some can be replays of old data we may not have any more.
        // If it turns out we don't have the block after all, we'll figure it
        // out in the `CreateBlock` effect, at which point we can time out
        // and sync with the `Prepare` message from the next leader.
        enqueueEvent(message)
    }
  }

  /** Trace an invalid message. Could include other penalties as well to the sender. */
  private def protocolError(
      error: ProtocolError[A]
  ): F[Unit] =
    tracers.rejected(error)

  /** Add a Prepare message to the synchronisation and validation queue.
    *
    * The High Q.C. in the message proves that the parent block is valid
    * according to the federation members.
    *
    * Any missing dependencies should be downloaded and the application asked
    * to validate each block in succession as the downloads are finished.
    */
  private def syncAndValidatePrepare(
      sender: A#PKey,
      prepare: Message.Prepare[A]
  ): F[Unit] =
    syncPipe.send(SyncPipe.PrepareRequest(sender, prepare))

  /** Process the synchronization result queue. */
  private def processSyncPipe: F[Unit] =
    syncPipe.receive
      .mapEval[Unit] {
        case SyncPipe.PrepareResponse(request, isValid) =>
          if (isValid) {
            enqueueEvent(
              validated(Event.MessageReceived(request.sender, request.prepare))
            )
          } else {
            protocolError(
              ProtocolError.UnsafeExtension(request.sender, request.prepare)
            )
          }

        case SyncPipe.StatusResponse(status) =>
          fastForwardState(status)
      }
      .completedL

  /** Replace the current protocol state based on what was synced with the federation. */
  private def fastForwardState(status: Status[A]): F[Unit] = {
    stateRef.get.flatMap { state =>
      val forward = state.copy[A](
        viewNumber = status.viewNumber,
        prepareQC = status.prepareQC,
        commitQC = status.commitQC
      )
      // Trigger the next view, so we get proper tracing and effect execution.
      tracers.adoptView(status) >>
        handleTransition(
          forward.handleNextView(Event.NextView(status.viewNumber))
        )
    }
  }

  /** Add a validated event to the queue for processing against the protocol state. */
  private def enqueueEvent(event: Validated[Event[A]]): F[Unit] =
    eventQueue.offer(event)

  /** Take a single event from the queue, apply it on the state,
    * kick off the resulting effects, then recurse.
    *
    * The effects will communicate their results back to the state
    * through the event queue.
    */
  private def processEvents: F[Unit] = {
    eventQueue.poll.flatMap { event =>
      stateRef.get.flatMap { state =>
        val handle: F[Unit] = event match {
          case Event.NextView(viewNumber) if viewNumber < state.viewNumber =>
            ().pure[F]

          case e @ Event.NextView(viewNumber) =>
            for {
              counter <- counterRef.get
              _       <- tracers.timeout(viewNumber -> counter)
              _       <- maybeRequestStatusSync(viewNumber, counter)
              _       <- handleTransition(state.handleNextView(e))
            } yield ()

          case e @ Event.MessageReceived(_, _) =>
            handleTransitionAttempt(
              state.handleMessage(Validated[Event.MessageReceived[A]](e))
            )

          case e @ Event.BlockCreated(_, _, _) =>
            handleTransition(state.handleBlockCreated(e))
        }

        handle >> processEvents
      }
    }
  }

  /** Request view state synchronisation if we timed out and it looks like we're out of sync. */
  private def maybeRequestStatusSync(
      viewNumber: ViewNumber,
      counter: MessageCounter
  ): F[Unit] = {
    // Only requesting a state sync if we haven't received any message that looks to be in sync
    // but we have received some from the future. If we have received messages from the past,
    // then by the virtue of timeouts they should catch up with us at some point.
    val isOutOfSync = counter.present == 0 && counter.future > 0

    // In the case that there were two groups being in sync within group members, but not with
    // each other, than there should be rounds when none of them are leaders and they shouldn't
    // receive valid present messages.
    val requestSync =
      tracers.viewSync(viewNumber) >>
        syncPipe.send(SyncPipe.StatusRequest(viewNumber))

    requestSync.whenA(isOutOfSync)
  }

  /** Handle successful state transition:
    * - apply local effects on the state
    * - schedule other effects to execute in the background
    * - if there was a phase or view transition, unstash delayed events
    */
  private def handleTransition(
      transition: ProtocolState.Transition[A]
  ): F[Unit] = {
    val (state, effects) = transition

    // Apply local messages to the state before anything else.
    val (nextState, nextEffects) =
      applySyncEffects(state, effects)

    // Unstash messages before we change state.
    captureChanges(nextState) >>
      unstash(nextState) >>
      stateRef.set(nextState) >>
      scheduleEffects(nextEffects)
  }

  /** Update the view state with and trace changes when they happen. */
  private def captureChanges(nextState: ProtocolState[A]): F[Unit] = {
    stateRef.get.flatMap { state =>
      def ifChanged[T](get: ProtocolState[A] => T)(f: T => F[Unit]) = {
        val prev = get(state)
        val next = get(nextState)
        f(next).whenA(prev != next)
      }

      ifChanged(_.viewNumber)(_ => counterRef.set(MessageCounter.empty)) >>
        ifChanged(_.viewNumber)(updateViewNumber) >>
        ifChanged(_.prepareQC)(updateQuorum) >>
        ifChanged(_.lockedQC)(updateQuorum) >>
        ifChanged(_.commitQC)(updateQuorum)
    }
  }

  private def updateViewNumber(viewNumber: ViewNumber): F[Unit] =
    tracers.newView(viewNumber) >>
      storeRunner.runReadWrite {
        viewStateStorage.setViewNumber(viewNumber)
      }

  private def updateQuorum(quorumCertificate: QuorumCertificate[A]): F[Unit] =
    tracers.quorum(quorumCertificate) >>
      storeRunner.runReadWrite {
        viewStateStorage.setQuorumCertificate(quorumCertificate)
      }

  /** Requeue messages which arrived too early, but are now due becuase
    * the state caught up with them.
    */
  private def unstash(nextState: ProtocolState[A]): F[Unit] =
    stateRef.get.flatMap { state =>
      val requeue = for {
        dueEvents <- stashRef.modify {
          _.unstash(nextState.viewNumber, nextState.phase)
        }
        _ <- dueEvents.traverse(e => enqueueEvent(validated(e)))
      } yield ()

      requeue.whenA(
        nextState.viewNumber != state.viewNumber || nextState.phase != state.phase
      )
    }

  /** Carry out local effects before anything else,
    * to eliminate race conditions when a vote sent
    * to self would have caused a state transition.
    *
    * Return the updated state and the effects to be
    * carried out asynchornously.
    */
  private def applySyncEffects(
      state: ProtocolState[A],
      effects: Seq[Effect[A]]
  ): ProtocolState.Transition[A] = {
    @tailrec
    def loop(
        state: ProtocolState[A],
        effectQueue: Queue[Effect[A]],
        asyncEffects: List[Effect[A]]
    ): ProtocolState.Transition[A] =
      effectQueue.dequeueOption match {
        case None =>
          (state, asyncEffects.reverse)

        case (Some((effect, effectQueue))) =>
          effect match {
            case Effect.SendMessage(recipient, message)
                if recipient == publicKey =>
              val event = Event.MessageReceived(recipient, message)

              state.handleMessage(validated(event)) match {
                case Left(_) =>
                  // This shouldn't happen, but let's just skip this event here and redeliver it later.
                  loop(state, effectQueue, effect :: asyncEffects)

                case Right((state, effects)) =>
                  loop(state, effectQueue ++ effects, asyncEffects)
              }

            case _ =>
              loop(state, effectQueue, effect :: asyncEffects)
          }
      }

    loop(state, Queue(effects: _*), Nil)
  }

  /** Try to apply a transition:
    * - if it's `TooEarly`, add it to the delayed stash
    * - if it's another error, ignore the event
    * - otherwise carry out the transition
    */
  private def handleTransitionAttempt(
      transitionAttempt: ProtocolState.TransitionAttempt[A]
  ): F[Unit] = transitionAttempt match {
    case Left(error @ ProtocolError.TooEarly(_, _, _)) =>
      tracers.stashed(error) >>
        stashRef.update { _.stash(error) }

    case Left(error) =>
      protocolError(error)

    case Right(transition) =>
      handleTransition(transition)
  }

  /** Effects can be processed independently of each other in the background. */
  private def scheduleEffects(effects: Seq[Effect[A]]): F[Unit] =
    effects.toList.traverse(scheduleEffect).void

  /** Start processing an effect in the background. Add the background fiber
    * to the scheduled items so they can be canceled if the service is released.
    */
  private def scheduleEffect(effect: Effect[A]): F[Unit] = {
    fiberSet.submit(processEffect(effect)).void
  }

  /** Process a single effect. This will always be wrapped in a Fiber. */
  private def processEffect(effect: Effect[A]): F[Unit] = {
    import Event._
    import Effect._

    val process = effect match {
      case ScheduleNextView(viewNumber, timeout) =>
        val event = validated(NextView(viewNumber))
        Timer[F].sleep(timeout) >> enqueueEvent(event)

      case CreateBlock(viewNumber, highQC) =>
        // Ask the application to create a block for us.
        appService.createBlock(highQC).flatMap {
          case None =>
            ().pure[F]

          case Some(block) =>
            enqueueEvent(
              validated(Event.BlockCreated(viewNumber, block, highQC))
            )
        }

      case SaveBlock(preparedBlock) =>
        storeRunner.runReadWrite {
          blockStorage.put(preparedBlock)
        }

      case effect @ ExecuteBlocks(_, _) =>
        // Each node may be at a different point in the chain, so how
        // long the executions take can vary. We could execute it in
        // the forground here, but it may cause the node to lose its
        // sync with the other federation members, so the execution
        // should be offloaded to another queue.
        blockExecutor.enqueue(effect)

      case SendMessage(recipient, message) =>
        network.sendMessage(recipient, message)
    }

    process.handleErrorWith { case NonFatal(ex) =>
      tracers.error(s"Error processing effect $effect", ex)
    }
  }

  private def validated(event: Event[A]): Validated[Event[A]] =
    Validated[Event[A]](event)

  private def validated(
      event: Event.MessageReceived[A]
  ): Validated[Event.MessageReceived[A]] =
    Validated[Event.MessageReceived[A]](event)
}

object ConsensusService {

  /** Stash to keep too early messages to be re-queued later.
    *
    * Every slot just has 1 place per federation member to avoid DoS attacks.
    */
  case class MessageStash[A <: Agreement](
      slots: Map[(ViewNumber, Phase), Map[A#PKey, Message[A]]]
  ) {
    def stash(error: ProtocolError.TooEarly[A]): MessageStash[A] = {
      val slotKey = (error.expectedInViewNumber, error.expectedInPhase)
      val slot    = slots.getOrElse(slotKey, Map.empty)
      copy(slots =
        slots.updated(
          slotKey,
          slot.updated(error.event.sender, error.event.message)
        )
      )
    }

    def unstash(
        dueViewNumber: ViewNumber,
        duePhase: Phase
    ): (MessageStash[A], List[Event.MessageReceived[A]]) = {
      val dueKeys = slots.keySet.filter { case (viewNumber, phase) =>
        viewNumber < dueViewNumber ||
          viewNumber == dueViewNumber &&
          !phase.isAfter(duePhase)
      }

      val dueEvents = dueKeys.toList.map(slots).flatten.map {
        case (sender, message) => Event.MessageReceived(sender, message)
      }

      copy(slots = slots -- dueKeys) -> dueEvents
    }
  }
  object MessageStash {
    def empty[A <: Agreement] = MessageStash[A](Map.empty)
  }

  /** Count the number of messages received from others in a round,
    * to determine whether we're out of sync or not in case of a timeout.
    */
  case class MessageCounter(
      past: Int,
      present: Int,
      future: Int
  ) {
    def incPast    = copy(past = past + 1)
    def incPresent = copy(present = present + 1)
    def incFuture  = copy(future = future + 1)
  }
  object MessageCounter {
    val empty = MessageCounter(0, 0, 0)
  }

  /** Create a `ConsensusService` instance and start processing events
    * in the background, shutting processing down when the resource is
    * released.
    *
    * `initState` is expected to be restored from persistent storage
    * instances upon restart.
    */
  def apply[
      F[_]: Timer: Concurrent: ContextShift,
      N,
      A <: Agreement: Block: Signing
  ](
      publicKey: A#PKey,
      network: Network[F, A#PKey, Message[A]],
      appService: ApplicationService[F, A],
      blockExecutor: BlockExecutor[F, N, A],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      syncPipe: SyncPipe[F, A]#Left,
      initState: ProtocolState[A],
      maxEarlyViewNumberDiff: Int = 1
  )(implicit
      tracers: ConsensusTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): Resource[F, ConsensusService[F, N, A]] =
    for {
      fiberSet <- FiberSet[F]

      service <- Resource.liftF(
        build[F, N, A](
          publicKey,
          network,
          appService,
          blockExecutor,
          blockStorage,
          viewStateStorage,
          syncPipe,
          initState,
          maxEarlyViewNumberDiff,
          fiberSet
        )
      )

      _ <- Concurrent[F].background(service.processNetworkMessages)
      _ <- Concurrent[F].background(service.processSyncPipe)
      _ <- Concurrent[F].background(service.processEvents)

      initEffects = ProtocolState.init(initState)
      _ <- Resource.liftF(service.scheduleEffects(initEffects))
    } yield service

  private def build[
      F[_]: Timer: Concurrent: ContextShift,
      N,
      A <: Agreement: Block: Signing
  ](
      publicKey: A#PKey,
      network: Network[F, A#PKey, Message[A]],
      appService: ApplicationService[F, A],
      blockExecutor: BlockExecutor[F, N, A],
      blockStorage: BlockStorage[N, A],
      viewStateStorage: ViewStateStorage[N, A],
      syncPipe: SyncPipe[F, A]#Left,
      initState: ProtocolState[A],
      maxEarlyViewNumberDiff: Int,
      fiberSet: FiberSet[F]
  )(implicit
      tracers: ConsensusTracers[F, A],
      storeRunner: KVStoreRunner[F, N]
  ): F[ConsensusService[F, N, A]] =
    for {
      stateRef   <- Ref[F].of(initState)
      stashRef   <- Ref[F].of(MessageStash.empty[A])
      fibersRef  <- Ref[F].of(Set.empty[Fiber[F, Unit]])
      counterRef <- Ref[F].of(MessageCounter.empty)
      eventQueue <- ConcurrentQueue[F].unbounded[Event[A]](None)

      service = new ConsensusService(
        publicKey,
        network,
        appService,
        blockExecutor,
        blockStorage,
        viewStateStorage,
        stateRef,
        stashRef,
        counterRef,
        syncPipe,
        eventQueue,
        fiberSet,
        maxEarlyViewNumberDiff
      )
    } yield service
}
