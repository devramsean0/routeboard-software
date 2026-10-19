{ inputs, ... }:
{
  imports = [
    inputs.rust-flake.flakeModules.default
    inputs.rust-flake.flakeModules.nixpkgs
  ];
  perSystem = { config, self', pkgs, lib, ... }: {
    rust-project.crates."routeboard-software".crane.args = {
      buildInputs = [ pkgs.openssl pkgs.rdkafka ];
      nativeBuildInputs = [ pkgs.pkg-config ];
      RDKAFKA_DYNAMIC_LINKING = "1";
      RDKAFKA_BUILD_STATIC = "0";

      # On darwin, you may need framework dependencies like IOKit.
      # The default SDK now provides these automatically - no need to specify them.
      # buildInputs = lib.optionals pkgs.stdenv.isDarwin [ pkgs.apple-sdk ];
    };
    packages.default = self'.packages.routeboard-software;
  };
}
