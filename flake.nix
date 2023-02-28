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
      in
      {
        devShells = rec {
          default = rust-dev;
          rust-dev = pkgs.mkShell {
            buildInputs = with pkgs; [
              rust-bin.beta.latest.default
              rust-analyzer
              clippy
              rustfmt
            ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              libiconv
            ];

            shellHook = ":";

            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
          };

        };
      }
    );
}
