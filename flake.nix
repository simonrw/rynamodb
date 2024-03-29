{
  description = "Flake utils demo";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          rust-overlay.overlays.default
        ];

        pkgs = import nixpkgs {
          inherit overlays system;
        };

        darwin-packages = with pkgs; with pkgs.darwin.apple_sdk; [
          libiconv
          frameworks.Security
          frameworks.CoreFoundation
          frameworks.Security
        ];

        testPython = pkgs.python3.withPackages (ps: with ps; [
          boto3
          pynamodb
          pytest
          pytest-instafail
          pytest-randomly
          pytest-timeout
          pytest-xdist
          requests
        ]);
      in
      {
        devShells = rec {
          default = rust-dev;
          rust-dev = pkgs.mkShell {
            buildInputs = with pkgs; [
              rustup
              rust-analyzer
              clippy
              cargo-insta
              rustfmt
              testPython
              ruff
              black
              sqlite
              rlwrap
            ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin darwin-packages;

            shellHook = ":";

            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
            RUST_LOG = "rynamodb=debug";
          };

        };
      }
    );
}
