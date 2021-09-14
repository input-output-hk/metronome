{
  description = "Metronome";

  inputs = {
    utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs, utils }:
    let
      localOverlay = import ./nix/overlay.nix;
      pkgsForSystem = system: import nixpkgs {
        overlays = [
          localOverlay
        ];
        inherit system;
      };
    in utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" ] (system: rec {
      legacyPackages = pkgsForSystem system;
      packages = utils.lib.flattenTree {
        inherit (legacyPackages) devShell metronome;
      };
      defaultPackage = packages.metronome;
      apps.metronome = utils.lib.mkApp { drv = packages.metronome; };  # use as `nix run <mypkg>`
      hydraJobs = { inherit (legacyPackages) metronome; };
  }) // {
    overlay = localOverlay;
  };
}
