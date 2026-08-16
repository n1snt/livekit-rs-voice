//! Locates `libmp3lame` at build time (via pkg-config, with common-path
//! fallbacks) so the FFI in `src/mp3.rs` links against it.

fn main() {
    // pkg-config first (Debian `libmp3lame-dev`, Homebrew `lame`).
    if pkg_config::Config::new()
        .atleast_version("3.0")
        .probe("mp3lame")
        .is_ok()
        || pkg_config::Config::new()
            .atleast_version("3.0")
            .probe("lame")
            .is_ok()
    {
        return;
    }
    // Fallbacks for environments without pkg-config.
    for dir in [
        "/opt/homebrew/lib",
        "/usr/local/lib",
        "/usr/lib",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
    ] {
        if std::path::Path::new(dir).join("libmp3lame.so").exists()
            || std::path::Path::new(dir).join("libmp3lame.dylib").exists()
            || std::path::Path::new(dir).join("libmp3lame.a").exists()
        {
            println!("cargo:rustc-link-search=native={dir}");
            println!("cargo:rustc-link-lib=dylib=mp3lame");
            return;
        }
    }
    panic!("libmp3lame not found; install libmp3lame-dev (Debian) or lame (Homebrew)");
}
