{ stdenv
, lib
, mill-derivation
}:

mill-derivation rec {
  pname = "metronome";
  version = "0.0.1";

  src = builtins.path { path = ../.; name = "source"; };

  depsWarmupTarget = "'metronome[2.13.4].examples.compile'";
  depsSha256 = "sha256-Utb4qPoFe0qC7TyhwdVG3Z0wBWUwJuZhFVy8xFW++RI=";

  millTarget = "'metronome[2.13.4].examples.assembly'";


}
