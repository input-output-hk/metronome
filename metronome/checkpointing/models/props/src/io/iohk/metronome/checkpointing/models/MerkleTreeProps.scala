package io.iohk.metronome.checkpointing.models

import org.scalacheck.{Gen, Properties}
import org.scalacheck.Arbitrary.arbitrary
import ArbitraryInstances.arbMerkleHash
import org.scalacheck.Prop.forAll

object MerkleTreeProps extends Properties("MerkleTree") {

  def genElements(max: Int = 256): Gen[List[MerkleTree.Hash]] =
    Gen.choose(0, max).flatMap { n =>
      Gen.listOfN(n, arbitrary(arbMerkleHash))
    }

  property("inclusionProof") = forAll(genElements()) { elements =>
    val merkleTree = MerkleTree.build(elements)
    elements.zipWithIndex.forall { case (elem, idx) =>
      val fromHash  = MerkleTree.generateProofFromHash(merkleTree, elem)
      val fromIndex = MerkleTree.generateProofFromIndex(merkleTree, idx)
      fromHash == fromIndex && fromHash.isDefined
    }
  }

  property("proofVerification") = forAll(genElements()) { elements =>
    val merkleTree = MerkleTree.build(elements)
    elements.forall { elem =>
      val maybeProof = MerkleTree.generateProofFromHash(merkleTree, elem)
      maybeProof.exists(MerkleTree.verifyProof(_, merkleTree.hash, elem))
    }
  }

  property("noFalseInclusion") = forAll(genElements(128), genElements(32)) {
    (elements, other) =>
      val nonElements = other.diff(elements)
      val merkleTree  = MerkleTree.build(elements)

      val noFalseProof = nonElements.forall { nonElem =>
        MerkleTree.generateProofFromHash(merkleTree, nonElem).isEmpty
      }

      val noFalseVerification = elements.forall { elem =>
        val proof = MerkleTree.generateProofFromHash(merkleTree, elem).get
        !nonElements.exists(MerkleTree.verifyProof(proof, merkleTree.hash, _))
      }

      noFalseProof && noFalseVerification
  }

  property("emptyTree") = {
    val empty = MerkleTree.build(Nil)

    MerkleTree.generateProofFromHash(empty, MerkleTree.empty.hash).isEmpty &&
    empty.hash == MerkleTree.empty.hash
  }

  property("singleElementTree") = forAll(arbMerkleHash.arbitrary) { elem =>
    val tree = MerkleTree.build(elem :: Nil)

    tree.hash == elem &&
    MerkleTree
      .generateProofFromHash(tree, elem)
      .map(MerkleTree.verifyProof(_, tree.hash, elem))
      .contains(true)
  }
}
