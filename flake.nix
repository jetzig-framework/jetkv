{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    zig-overlay.url = "github:mitchellh/zig-overlay";
    zig-overlay.inputs = {
      nixpkgs.follows = "nixpkgs";
      flake-utils.follows = "flake-utils";
    };
    zls-flake.url = "github:zigtools/zls";
    zls-flake.inputs = {
      nixpkgs.follows = "nixpkgs";
      zig-overlay.follows = "zig-overlay";
    };
  };

  outputs = { self, nixpkgs, flake-utils, zig-overlay, zls-flake }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        zigVersion = "master";

        pkgs = nixpkgs.legacyPackages.${system};
        zig = zig-overlay.packages.${system}.${zigVersion};
        zls = zls-flake.packages.${system}.zls;

        valkeySetup = pkgs.writeShellScriptBin "valkey-setup" ''
          JETKV_TEST_DIR="$TMPDIR/jetkv-env"
          VALKEY_DIR="$JETKV_TEST_DIR/valkey-db"
          mkdir -p "$VALKEY_DIR"
          cat > "$VALKEY_DIR/valkey.conf" << EOF
port 6379
dir $VALKEY_DIR
dbfilename dump.rdb
logfile $VALKEY_DIR/valkey.log
daemonize yes
pidfile $VALKEY_DIR/valkey.pid
save 900 1
save 300 10
save 60 10000
stop-writes-on-bgsave-error no
EOF
          valkey-server "$VALKEY_DIR/valkey.conf"
          echo "Valkey started (pid: $(cat $VALKEY_DIR/valkey.pid))"
        '';

        valkeyTeardown = pkgs.writeShellScriptBin "valkey-teardown" ''
          JETKV_TEST_DIR="$TMPDIR/jetkv-env"
          VALKEY_DIR="$JETKV_TEST_DIR/valkey-db"
          if [ -f "$VALKEY_DIR/valkey.pid" ]; then
            kill "$(cat "$VALKEY_DIR/valkey.pid")"
            echo "Valkey stopped"
          fi
          if [ -d "$VALKEY_DIR" ]; then
            rm -rf "$VALKEY_DIR"
          fi
        '';

      in {
        devShells.default = pkgs.mkShell {
          name = "jetkv-dev";
          buildInputs = [
            zig
            zls
            pkgs.valkey
            valkeySetup
            valkeyTeardown
          ];
          shellHook = ''
            JETKV_TEST_DIR="$TMPDIR/jetkv-env"
            mkdir -p "$JETKV_TEST_DIR"
            echo "jetkv dev env"
            echo "  valkey-setup    start valkey on :6379"
            echo "  valkey-teardown stop valkey and clean up"
            trap valkey-teardown EXIT
          '';
        };
      });
}
