package io.iohk.metronome.hotstuff.consensus

/** Collection of keys of the federation members.
  *
  * There are two inequalities that decide the quorum size `q`:
  *
  * 1.) Safety inequality:
  *   There should not be two conflicting quorums.
  *   If two quorums conflict, their intersection has a size of `2q-n`.
  *   The intersection represents equivocation and will have a size of
  *   at most `f` (since honest nodes don't equivocate).
  *   Thus for safety we need `2q-n > f` => `q > (n+f)/2`
  *
  * 2.) Liveness inequality:
  *   Quorum size should be small enough so that adversaries cannot deadlock
  *   the system by not voting. If the quorum size is greater than `n-f`,
  *   adversaries may decide to not vote and hence we will not have any quorum certificate.
  *   Thus, we need `q <= n-f`
  *
  * So any `q` between `(n+f)/2+1` and `n-f` should work.
  * Smaller `q` is preferred as it would improve speed.
  * We can set it to `(n+f)/2+1` or fix it to `2/3n+1`.
  *
  * Extra: The above two inequalities `(n+f)/2 < q <= n-f`, lead to the constraint: `f < n/3`, or `n >= 3*f+1`.
  */
abstract case class Federation[PKey] private (
    publicKeys: IndexedSeq[PKey],
    // Maximum number of Byzantine nodes.
    maxFaulty: Int
)(implicit ls: LeaderSelection) {
  private val publicKeySet = publicKeys.toSet

  /** Size of the federation. */
  val size: Int = publicKeys.size

  /** Number of signatures required for a Quorum Certificate. */
  val quorumSize: Int = (size + maxFaulty) / 2 + 1

  def contains(publicKey: PKey): Boolean =
    publicKeySet.contains(publicKey)

  def leaderOf(viewNumber: ViewNumber): PKey =
    publicKeys(implicitly[LeaderSelection].leaderOf(viewNumber, size))
}

object Federation {

  /** Create a federation with the highest possible fault tolerance. */
  def apply[PKey](
      publicKeys: IndexedSeq[PKey]
  )(implicit ls: LeaderSelection): Either[String, Federation[PKey]] =
    apply(publicKeys, maxByzantine(publicKeys.size))

  /** Create a federation with the fault tolerance possibly reduced from the theoretical
    * maximum, which can allow smaller quorum sizes, and improved speed.
    *
    * Returns an error if the configured value is higher than the theoretically tolerable maximum.
    */
  def apply[PKey](
      publicKeys: IndexedSeq[PKey],
      maxFaulty: Int
  )(implicit ls: LeaderSelection): Either[String, Federation[PKey]] = {
    val f = maxByzantine(publicKeys.size)
    if (publicKeys.isEmpty) {
      Left("The federation cannot be empty!")
    } else if (publicKeys.distinct.size < publicKeys.size) {
      Left("The keys in the federation must be unique!")
    } else if (maxFaulty > f) {
      Left(
        s"The maximum tolerable number of Byzantine members is $f, less than the specified $maxFaulty."
      )
    } else {
      Right(new Federation(publicKeys, maxFaulty) {})
    }
  }

  /** Maximum number of Byzantine nodes in a federation of size `n` */
  private def maxByzantine(n: Int): Int = (n - 1) / 3
}
