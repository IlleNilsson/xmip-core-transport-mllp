//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). It names nothing of HL7's, whose acknowledgement only the
//! code writes.

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

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["mllp"];
