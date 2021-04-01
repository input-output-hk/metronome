package io.iohk.metronome.checkpointing

import io.iohk.ethereum.crypto.ECDSASignature
import io.iohk.metronome.hotstuff.consensus.basic.Agreement
import org.bouncycastle.crypto.params.{
  ECPublicKeyParameters,
  ECPrivateKeyParameters
}

trait CheckpointingAgreement extends Agreement {
  override type Block = models.Block
  override type Hash  = models.Block.Header.Hash
  override type PSig  = ECDSASignature
  // TODO (PM-2935): Replace list with theshold signatures.
  override type GSig = List[ECDSASignature]
  override type PKey = ECPublicKeyParameters
  override type SKey = ECPrivateKeyParameters
}
