# Metronome

Metronome is a checkpointing component for Proof-of-Work blockchains, using the [HotStuff BFT](https://arxiv.org/pdf/1803.05069.pdf) algorithm.

## Overview
Checkpoints provides finality to blockchains by attesting to the hash of well-embedded blocks. A proper checkpointing system can secure the blockchain even under adversary with super-majority mining power.

The Metronome checkpointing system consists of a generic BFT Service (preferrably HotStuff), a Checkpoint-assisted Blockchain, and a Checkpointing Intepreter that bridges the two. This structure enables many features, including flexible BFT choices, multi-chain support, plug-and-play forensic monitoring platform via the BFT service, as well as the ability of bridging trust between two different blockchains.

### Architecture

BFT Service: Committee-based BFT service with a simple and generic interface: It takes consensus candidates (e.g., checkpoint candidates) as input and generates certificates for the elected ones.

Checkpoint-assisted Blockchain: Maintains the main blockchain that accepts and applies checkpointing results. The checkpointing logic is delegated to the checkpointing interpreter below.

Checkpointing Interpreter: Maintains checkpointing logic, including the creation and validation (via blockchain) of checkpointing candidates, as well as checkpoint-related validation of new blockchain blocks.

Each of these modules can be developed independently with only minor data structure changes required for compatibility. This independence allows us to be flexible with the choice of BFT algorithm (e.g., variants of OBFT or Hotstuff) and checkpointing interpreter (e.g., simple checkpoints or Advocate).

The architecture also enables a convenient forensic monitoring module: By simply connecting to the BFT service, the forensics module can download the stream of consensus data and detect illegal behaviors such as collusion and identify the offenders.

![](docs/architecture.png)

### BFT Algorithm

The BFT service delegates checkpoint proposal and candidate validation to the Checkpointing Interpreter using 2-way communication to allow asynchronous responses as and when the data becomes available:

![](docs/master-based.png)

When a winner is elected a Checkpoint Certificate is compiled, comprising of the checkpointed data (a block identity, or something more complex) as well as a witness for the BFT agreement, which proves that the decision is final and cannot be rolled back. Because of the need for this proof, low latency BFT algorithms such as HotStuff are preferred.


## Build

The project is built using [mill](https://github.com/com-lihaoyi/mill), which works fine with [Metals](https://scalameta.org/metals/docs/build-tools/mill.html).

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
mill __.checkpointing.app.test.single io.iohk.metronome.app.config.ConfigSpec
```

### Formatting the codebase

Please configure your editor to use `scalafmt` on save. CI will be configured to check formatting.


## Publishing

We're using the [VersionFile](https://com-lihaoyi.github.io/mill/page/contrib-modules.html#version-file) plugin to manage versions.

The initial version has been written to the file without newlines:
```console
echo -n "0.1.0-SNAPSHOT" > versionFile/version
```

Builds on `develop` will publish the snapshot version to Sonatype, which can be overwritten if the version number isn't updated.

During [publishing](https://com-lihaoyi.github.io/mill/page/common-project-layouts.html#publishing) on `master` we'll use `mill versionFile.setReleaseVersion` to remove the `-SNAPSHOT` postfix and make a release. After that the version number should be bumped on `develop`, e.g. `mill versionFile.setNextVersion --bump minor`.
