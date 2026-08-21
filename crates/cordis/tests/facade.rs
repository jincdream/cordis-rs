use cordis::Context;

#[test]
fn facade_exports_core() {
    assert_eq!(Context::new().fiber().id(), Some(cordis::FiberId(0)));
}

#[cfg(feature = "full")]
#[test]
fn full_feature_exports_every_standard_crate() {
    let _ = cordis::group::BUILTIN_NAME;
    let _ = cordis::loader::EntryOptions::default();
    let _ = cordis::include::PatchOptions::default();
    let _ = cordis::hmr::WatchTarget::new("plugin.rs", "plugin.rs");
    let _ = cordis::logger_console::ConsoleConfig::default();
    let context = Context::new();
    let _ = cordis::timer::TimerService::new(context.clone());
    let _ = cordis::utils::List::<u8>::new(context, "facade test");
}
