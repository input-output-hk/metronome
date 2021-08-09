{ lib, stdenv, mill, git, gnused, zstd, strip-nondeterminism, file, findutils, strace }:

{ name ? "${args'.pname}-${args'.version}", src, nativeBuildInputs ? [ ]
, passthru ? { }, patches ? [ ]

  # A function to override the dependencies derivation
, overrideDepsAttrs ? (_oldAttrs: { })

# depsSha256 is the sha256 of the dependencies
, depsSha256

# whether to put the version in the dependencies' derivation too or not.
# every time the version is changed, the dependencies will be re-downloaded
, versionInDepsName ? false

  # command to run to let mill fetch all the required dependencies for the build.
, depsWarmupTarget ? "__.compile"

, millTarget ? "__.assembly"

, ... }@args':

with builtins;
with lib;

let
  args =
    removeAttrs args' [ "overrideDepsAttrs" "depsSha256" ];

  deps = let
    depsAttrs = {
      name = "${if versionInDepsName then name else args'.pname}-deps";
      inherit src patches;

      nativeBuildInputs = [ mill git gnused zstd strip-nondeterminism file findutils ]
        ++ nativeBuildInputs;

      outputHash = depsSha256;
      outputHashAlgo = "sha256";
      outputHashMode = "recursive";

      impureEnvVars = lib.fetchers.proxyImpureEnvVars
        ++ [ "GIT_PROXY_COMMAND" "SOCKS_SERVER" ];

      postBuild = args.postBuild or ''
        runHook preBuild

        # clean up any lingering build artifacts
        rm -rf out

        echo running \"mill ${depsWarmupTarget}\" to warm up the caches

        export HOME=$NIX_BUILD_TOP/.nix
        export COURSIER_CACHE=$NIX_BUILD_TOP/.nix
        mill \
          --home $NIX_BUILD_TOP/.nix \
          -D coursier.home=$NIX_BUILD_TOP/.nix/coursier \
          -D ivy.home=$NIX_BUILD_TOP/.nix/.ivy2 \
          -D user.home=$NIX_BUILD_TOP/.nix \
          ${depsWarmupTarget}
      '';

      installPhase =''
        # each mill build will leave behind a worker directory
        # which will include an `io` socket, which will make the build fail
        rm -rf out/mill-worker*

        mkdir -pv $out/.nix $out/out
        cp -r $NIX_BUILD_TOP/.nix/* $out/.nix
        cp -r out/* $out/out/
      '';
    };
  in stdenv.mkDerivation (depsAttrs // overrideDepsAttrs depsAttrs);
in stdenv.mkDerivation (args // {
  nativeBuildInputs = [ mill zstd ] ++ nativeBuildInputs;

  postConfigure = (args.postConfigure or "") + ''
    echo extracting dependencies
    cp -r ${deps}/out out
    cp -r ${deps}/.nix $NIX_BUILD_TOP/.nix
    chmod -R +rwX $NIX_BUILD_TOP/.nix out
  '';

  buildPhase = (args.buildPhase or ''
    export HOME=$NIX_BUILD_TOP/.nix
    export COURSIER_CACHE=$NIX_BUILD_TOP/.nix
    mill \
      --home $NIX_BUILD_TOP/.nix \
      -D coursier.home=$NIX_BUILD_TOP/.nix/coursier \
      -D ivy.home=$NIX_BUILD_TOP/.nix/.ivy2 \
      -D user.home=$NIX_BUILD_TOP/.nix \
      ${depsWarmupTarget}
  '');

  preInstall = (args.preInstall or "") + ''
    # remove unneeded worker cache directory, including a socket which will cause nix to fail
    rm -rf out/mill-worker*
  '';

  # TODO: find a better way to escape '' in a lines block
  installPhase = args.installPhase or (throw ''
    Please specify an installPhase.
    There's not a good way to determine which artifacts are relevant.

    In general, installation of a "fat jar" will look something like:

    installPhase = ${"''"}
      mkdir -p $out/lib
      cp out/<project>/<output>/assembly/dest/out.jar $out/lib/

      makeWrapper ''${jdk11_headless}/bin/java $out/bin/''${pname} \
          --add-flags "-cp $out/lib/out.jar com.example.module.Class"
    ${"''"}
  '');

  passthru = { inherit deps; };
})
