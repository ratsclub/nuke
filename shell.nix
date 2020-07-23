{ pkgs ? import <nixpkgs> {} }:

with pkgs;
let
  my-python-packages = python-packages: with python-packages; [
    python-language-server
    requests
    tqdm
  ];
  python-with-my-packages = python37.withPackages my-python-packages;
in
pkgs.mkShell {
  buildInputs = [
    python-with-my-packages
  ];
}
