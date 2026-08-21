//! The standard group marker plugin used by `cordis-loader`.

use cordis_core::Plugin;

pub const BUILTIN_NAME: &str = "cordis:group";

/// Creates the group marker plugin.
///
/// Child reconciliation is owned by `cordis-loader`; the marker still receives a fiber so
/// lifecycle and configuration inspection match ordinary entries.
pub fn plugin() -> Plugin {
    Plugin::new("group", |_, _| async { Ok(None) })
}
