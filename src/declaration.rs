//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. It names
//! nothing of HL7's, whose acknowledgement only the code writes.

use xcore::settings::{Applies, Settings};

/// Every setting a Location takes beyond its address, which is where a
/// Receive Location listens and where a Send Location connects; the one
/// setting bounds the wait for either.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[transport::configured::timeout(
        Applies::Both,
        None,
        "How long a connection, a frame and its acknowledgement are waited on.",
    )],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["mllp"];
