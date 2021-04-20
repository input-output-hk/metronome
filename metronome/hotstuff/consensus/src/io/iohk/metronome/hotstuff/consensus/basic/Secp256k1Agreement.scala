package io.iohk.metronome.hotstuff.consensus.basic

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.crypto.{ECPrivateKey, ECPublicKey}

trait Secp256k1Agreement extends Agreement {
  override final type SKey = ECPrivateKey
  override final type PKey = ECPublicKey
  override final type PSig = ECDSASignature
  // TODO (PM-2935): Replace list with theshold signatures.
  override final type GSig = List[ECDSASignature]
}
