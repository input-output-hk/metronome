package io.iohk.metronome.hotstuff.service

import cats.implicits._
import cats.effect.{Concurrent, Timer, Fiber, Resource, ContextShift}
import cats.effect.concurrent.{Ref, Deferred}
import io.iohk.metronome.core.Validated
import io.iohk.metronome.hotstuff.consensus.basic.{
  Agreement,
  Effect,
  Event,
  ProtocolState,
  Phase,
  Message
}
import io.iohk.metronome.hotstuff.consensus.basic.QuorumCertificate
import monix.catnap.ConcurrentQueue
import io.iohk.metronome.networking.ConnectionHandler

/** An effectful executor wrapping the pure HotStuff ProtocolState.
  *
  * It handles the `consensus.basic.Message` events coming from the network.
  */
class ConsensusService[F[_]: Timer: Concurrent, A <: Agreement](
    network: Network[F, A, Message[A]],
    stateRef: Ref[F, ProtocolState[A]],
    fibersRef: Ref[F, Set[Fiber[F, Unit]]],
    eventQueue: ConcurrentQueue[F, Event[A]],
    blockExecutionQueue: ConcurrentQueue[F, Effect.ExecuteBlocks[A]]
) {

  /** Get the current protocol state, perhaps to respond to status requests. */
  def getState: F[ProtocolState[A]] =
    stateRef.get

  /** Process incoming network messages. */
  private def processMessages: F[Unit] = {
    network.incomingMessages
      .mapEval[Unit] { case ConnectionHandler.MessageReceived(from, message) =>
        // - Validate the message.
        // - Synchronise any missing block dependencies:
        //   - download blocks until we find a parent we have
        //   - validate blocks going from parent to children
        //     - if the block is invalid, stop downloading the branch
        //     - if the block is valid, enqueue it into the validated events queue
        val event = Event.MessageReceived(from, message)

        stateRef.get.flatMap { state =>
          state.validateMessage(event) match {
            case Left(error) =>
              ().pure[F]

            case Right(valid) =>
              // TODO (PM-3134): Sync dependencies.
              // NOTE: The Prepare message should have a Q.C. for the parent
              // of the block, so that shows that the block is legit.
              // Otherwise if we see a Vote with an unknown block we can ignore it,
              // and if it's a Quorum Certificate then we can download because again
              // we have proof for its legitimacy.
              enqueueEvent(valid)
          }
        }
      }
      .completedL
  }

  /** Add an event to the queue for later processing. */
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
        // - Try to apply the event on the current state
        //   - if it's `TooEarly`, add it to the delayed stash
        //   - if it's another error, ignore the event
        //   - if it's valid:
        //     - apply local effects on the state
        //     - schedule other effects to execute in the background
        //     - if there was a phase or view transition, unstash delayed events

        // TODO: Event handling.
        val (nextState, effects): (ProtocolState[A], Seq[Effect[A]]) =
          ???

        stateRef.set(nextState) >>
          scheduleEffects(effects) >>
          processEvents
      }
    }
  }

  /** Effects can be processed independently of each other in the background. */
  private def scheduleEffects(effects: Seq[Effect[A]]): F[Unit] =
    effects.toList.traverse(scheduleEffect).void

  /** Start processing an effect in the background. Add the background fiber
    * to the scheduled items so they can be canceled if the service is released.
    */
  private def scheduleEffect(effect: Effect[A]): F[Unit] = {
    for {
      deferredFiber <- Deferred[F, Fiber[F, Unit]]

      // Process the effect, then remove the fiber from the tracked tasks.
      task = for {
        _     <- processEffect(effect)
        fiber <- deferredFiber.get
        _     <- fibersRef.update(_ - fiber)
      } yield ()

      // Start the background task. Only now do we know the identity of the fiber.
      fiber <- Concurrent[F].start(task)

      // Add the fiber to the collectin first, so that if the effect is
      // already finished, it gets to remove it and we're not leaking memory.
      _ <- fibersRef.update(_ + fiber)
      _ <- deferredFiber.complete(fiber)
    } yield ()
  }

  /** Stop all background processing. */
  private def cancelEffects: F[Unit] =
    fibersRef.get.flatMap { fibers =>
      fibers.toList.traverse(_.cancel)
    }.void

  /** Process a single effect. This will always be wrapped in a Fiber. */
  private def processEffect(effect: Effect[A]): F[Unit] = {
    import Event._
    import Effect._

    effect match {
      case ScheduleNextView(viewNumber, timeout) =>
        val event = Validated[Event[A]](NextView(viewNumber))
        Timer[F].sleep(timeout) >> enqueueEvent(event)

      case CreateBlock(viewNumber, highQC) =>
        // Ask the application to create a block for us.
        // TODO (PM-3109): Create block.
        ???

      case effect @ ExecuteBlocks(_, commitQC) =>
        // Each node may be at a different point in the chain, so how
        // long the executions take can vary. We could execute it in
        // the forground here, but it may cause the node to lose its
        // sync with the other federation members, so the execution
        // should be offloaded to another queue.
        //
        // Save the Commit Qorum Certificate to the view state.
        saveCommitQC(commitQC) >>
          blockExecutionQueue.offer(effect)

      case SendMessage(recipient, message) =>
        network.sendMessage(recipient, message)
    }
  }

  /** Update the view state with the last Commit Quorum Certificate. */
  private def saveCommitQC(qc: QuorumCertificate[A]): F[Unit] = {
    assert(qc.phase == Phase.Commit)
    // TODO (PM-3112): Persist View State.
    ???
  }

  /** Execute blocks in order, updating pesistent storage along the way. */
  private def executeBlocks: F[Unit] = {
    blockExecutionQueue.poll.flatMap {
      case Effect.ExecuteBlocks(lastExecutedBlockHash, commitQC) =>
        // Retrieve the blocks from the storage from the last executed
        // to the one in the Quorum Certificate and tell the application
        // to execute them one by one. Update the persistent view state
        // after reach execution to remember which blocks we have truly
        // done.
        // TODO (PM-3133): Execute block
        ???
    } >> executeBlocks
  }
}

object ConsensusService {

  /** Create a `ConsensusService` instance and start processing events
    * in the background, shutting processing down when the resource is
    * released.
    *
    * `initState` is expected to be restored from persistent storage
    * instances upon restart.
    */
  def apply[F[_]: Timer: Concurrent: ContextShift, A <: Agreement](
      network: Network[F, A, Message[A]],
      initState: ProtocolState[A]
  ): Resource[F, ConsensusService[F, A]] =
    // TODO (PM-3187): Add Tracing
    for {
      service <- Resource.make(build[F, A](network, initState))(_.cancelEffects)
      _       <- Concurrent[F].background(service.processMessages)
      _       <- Concurrent[F].background(service.processEvents)
      _       <- Concurrent[F].background(service.executeBlocks)
      initEffects = ProtocolState.init(initState)
      _ <- Resource.liftF(service.scheduleEffects(initEffects))
    } yield service

  private def build[F[_]: Timer: Concurrent: ContextShift, A <: Agreement](
      network: Network[F, A, Message[A]],
      initState: ProtocolState[A]
  ): F[ConsensusService[F, A]] =
    for {
      stateRef   <- Ref[F].of(initState)
      fibersRef  <- Ref[F].of(Set.empty[Fiber[F, Unit]])
      eventQueue <- ConcurrentQueue[F].unbounded[Event[A]](None)
      blockExecutionQueue <- ConcurrentQueue[F]
        .unbounded[Effect.ExecuteBlocks[A]](None)
      service = new ConsensusService[F, A](
        network,
        stateRef,
        fibersRef,
        eventQueue,
        blockExecutionQueue
      )
    } yield service
}
