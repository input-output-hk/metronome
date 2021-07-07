# Metronome

Metronome is a checkpointing component for Proof-of-Work blockchains, using the [HotStuff BFT](https://arxiv.org/pdf/1803.05069.pdf) algorithm.

## Overview
Checkpointing provides finality to blockchains by attesting to the hash of well-embedded blocks. A proper checkpointing system can secure the blockchain even against an adversary with super-majority mining power.

The Metronome checkpointing system consists of a generic BFT Service (preferably HotStuff), a Checkpoint-assisted Blockchain, and a Checkpointing Interpreter that bridges the two. This structure enables many features, including flexible BFT choices, multi-chain support, plug-and-play forensic monitoring platform via the BFT service, and the capability of bridging trust between two different blockchains.

### Architecture

BFT Service: A committee-based BFT service with a simple and generic interface. It takes consensus candidates (e.g., checkpoint candidates) as input and generates certificates for the elected ones.

Checkpoint-assisted Blockchain: Maintains the main blockchain that accepts and applies checkpointing results. The checkpointing logic is delegated to the checkpointing interpreter below.

Checkpointing Interpreter: Maintains checkpointing logic, including the creation and validation (via blockchain) of checkpointing candidates, as well as checkpoint-related validation of new blockchain blocks.

Each of these modules can be developed independently with only minor data structure changes required for compatibility. This independence allows flexibility with the choice of BFT algorithm (e.g., variants of OBFT or Hotstuff) and checkpointing interpreter (e.g., simple checkpoints or Advocate).

The architecture also enables a convenient forensic monitoring module. By simply connecting to the BFT service, the forensics module can download the stream of consensus data and detect illegal behaviors such as collusion, and identify the offenders.

![Architecture diagram](docs/architecture.png)

![Component diagram](docs/components.png)

### BFT Algorithm

The BFT service delegates checkpoint proposal and candidate validation to the Checkpointing Interpreter using 2-way communication to allow asynchronous responses as and when the data becomes available.

![Algorithm diagram](docs/master-based.png)

When a winner is elected, a Checkpoint Certificate is compiled, comprising the checkpointed data (a block identity, or something more complex) and a witness for the BFT agreement, which proves that the decision is final and cannot be rolled back. Because of the need for this proof, low latency BFT algorithms such as HotStuff are preferred.


## Build

The project is built using [Mill](https://github.com/com-lihaoyi/mill), which works fine with [Metals](https://scalameta.org/metals/docs/build-tools/mill.html).

To compile everything, use the `__` wildcard:

```console
mill __.compile
```

The project is set up to cross build to all Scala versions for downstream projects that need to import the libraries. To build any specific version, put them in square brackets:

```console
mill metronome[2.12.10].checkpointing.app.compile
```

To run tests, use the wild cards again and the `.test` postix:

```console
mill __.test
mill --watch metronome[2.13.4].rocksdb.test
```

To run a single test class, use the `.single` method with the full path to the spec:

```console
mill __.storage.test.single io.iohk.metronome.storage.KVStoreStateSpec
```

To experiment with the code, start an interactive session:

```console
mill -i metronome[2.13.4].hotstuff.consensus.console
```

### Formatting the codebase

Please configure your editor to use `scalafmt` on save. CI will be configured to check formatting.


## Publishing

We're using the [VersionFile](https://com-lihaoyi.github.io/mill/page/contrib-modules.html#version-file) plugin to manage versions.

The initial version has been written to the file without newlines:
```console
echo -n "0.1.0-SNAPSHOT" > versionFile/version
```

Builds on `develop` will publish the snapshot version to Sonatype, which can be overwritten if the version number is not updated.

During [publishing](https://com-lihaoyi.github.io/mill/page/common-project-layouts.html#publishing) on `master` we will use `mill versionFile.setReleaseVersion` to remove the `-SNAPSHOT` postfix and make a release. After that the version number should be bumped on `develop`, e.g. `mill versionFile.setNextVersion --bump minor`.

## Example Applications

The [examples](./metronome/examples) module contains applications that demonstrate the software at work with simplified consensus use cases.

### Robot

The [robot example](./metronome/examples/src/io/iohk/metronome/examples/robot) is about the federation moving around a fictional robot on the screen.
Each leader proposes the next command to be carried out by the robot, once consensus is reached. The setup assumes 4 nodes, with at most
1 Byzantine member.

To test it, start 4 consoles and run commands like the following, `$i` going from 0 to 3:

```shell
./metronome/examples/robot.sh $i
```

The detailed logs should be in `~/.metronome/examples/robot/logs/node-$i.log`, e.g.:

```console
$ tail -f ~/.metronome/examples/robot/logs/node-0.log
14:03:03.607 ERROR i.i.m.h.s.tracing.ConsensusEvent Error {"message":"Error processing effect SendMessage(ECPublicKey(ByteVector(64 bytes, 0xcb020251d396614a35038dd2ff88fd2f1a5fd74c8bcad4b353fa605405c8b1b8c80ee12d2a10b1fca59424b16890c8115fbc94a68026369acc3c2603595e6387)),NewView(5,QuorumCertificate(Prepare,0,ByteVector(32 bytes, 0xb978b34fc4e905a727065b3e18d941a44a8349d8251514debbee5d6ddb94d430),GroupSignature(List()))))","error":"Connection with node ECPublicKey(ByteVector(64 bytes, 0xcb020251d396614a35038dd2ff88fd2f1a5fd74c8bcad4b353fa605405c8b1b8c80ee12d2a10b1fca59424b16890c8115fbc94a68026369acc3c2603595e6387)), has already closed"}
14:03:04.736 DEBUG i.i.m.networking.NetworkEvent ConnectionFailed {"key":"23fcab42e8f1078880b27aab4849092489bfa8d3e3b0faa54c9db89e89223c783ec7a3b2f8e6461b27778f78cea261a2272abe31c5601173b2964ef14af897dc","address":"localhost/127.0.0.1:40003","error":"io.iohk.scalanet.peergroup.PeerGroup$ChannelSetupException: Error establishing channel to PeerInfo(BitVector(512 bits, 0x23fcab42e8f1078880b27aab4849092489bfa8d3e3b0faa54c9db89e89223c783ec7a3b2f8e6461b27778f78cea261a2272abe31c5601173b2964ef14af897dc),localhost/127.0.0.1:40003)."}
```

To clear out everything before a restart, just run `rm -rf ~/.metronome/examples/robot`.
