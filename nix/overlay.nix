final: prev: {

  mill = prev.mill.override { jre = prev.jdk11; };

  mill-derivation = final.callPackage ./mill-derivation.nix { };

  metronome = final.callPackage ./metronome.nix { };

  devShell = with final; mkShell {
    nativeBuildInputs = [
      mill
    ];
  };
}
