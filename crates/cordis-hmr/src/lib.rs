//! Transactional hot module replacement with dependency propagation.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{DefaultHasher, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use cordis_core::{CordisError, Plugin, Result, shared};
use cordis_loader::{Loader, RegistryResolver};
use parking_lot::RwLock;

pub type PluginFactory = Arc<dyn Fn() -> Result<Plugin> + Send + Sync>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ChangeOutcome {
    FullReload,
    Ignored,
    Reloaded(Vec<String>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WatchTarget {
    pub path: PathBuf,
    pub url: String,
}

impl WatchTarget {
    pub fn new(path: impl Into<PathBuf>, url: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            url: url.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub changed: Vec<String>,
    pub outcome: Result<ChangeOutcome>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HmrReload {
    pub plugin_name: String,
    pub module_url: String,
}

pub struct WatchHandle {
    stop: tokio::sync::watch::Sender<bool>,
    events: tokio::sync::mpsc::UnboundedReceiver<WatchEvent>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl WatchHandle {
    pub async fn next(&mut self) -> Option<WatchEvent> {
        self.events.recv().await
    }

    pub async fn stop(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
    content_hash: u64,
}

#[derive(Clone)]
struct ModuleRecord {
    dependencies: HashSet<String>,
    plugin_name: Option<String>,
    factory: Option<PluginFactory>,
    external: bool,
}

#[derive(Clone)]
pub struct Hmr {
    loader: Loader,
    resolver: Arc<RegistryResolver>,
    modules: Arc<RwLock<HashMap<String, ModuleRecord>>>,
    transaction: Arc<tokio::sync::Mutex<()>>,
}

impl Hmr {
    pub fn new(loader: Loader, resolver: Arc<RegistryResolver>) -> Self {
        Self {
            loader,
            resolver,
            modules: Arc::new(RwLock::new(HashMap::new())),
            transaction: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn register_module(
        &self,
        url: impl Into<String>,
        dependencies: impl IntoIterator<Item = impl Into<String>>,
        plugin_name: Option<String>,
        factory: Option<PluginFactory>,
        external: bool,
    ) {
        self.modules.write().insert(
            url.into(),
            ModuleRecord {
                dependencies: dependencies.into_iter().map(Into::into).collect(),
                plugin_name,
                factory,
                external,
            },
        );
    }

    pub fn remove_module(&self, url: &str) {
        self.modules.write().remove(url);
    }

    /// Starts a portable polling watcher that batches file changes before replacement.
    pub async fn watch(
        &self,
        targets: impl IntoIterator<Item = WatchTarget>,
        poll_interval: Duration,
        debounce: Duration,
    ) -> Result<WatchHandle> {
        if poll_interval.is_zero() {
            return Err(CordisError::InvalidConfig(
                "HMR poll interval must be greater than zero".to_owned(),
            ));
        }
        let targets = targets.into_iter().collect::<Vec<_>>();
        let mut stamps = HashMap::new();
        for target in &targets {
            stamps.insert(target.url.clone(), file_stamp(&target.path).await?);
        }
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let (event_sender, events) = tokio::sync::mpsc::unbounded_channel();
        let hmr = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.tick().await;
            let mut pending = HashSet::new();
            let mut last_change = None;
            loop {
                tokio::select! {
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        for target in &targets {
                            let Ok(stamp) = file_stamp(&target.path).await else {
                                continue;
                            };
                            let previous = stamps.insert(target.url.clone(), stamp);
                            if previous.is_some_and(|previous| previous != stamp) {
                                pending.insert(target.url.clone());
                                last_change = Some(tokio::time::Instant::now());
                            }
                        }
                        let ready = last_change.is_some_and(|last_change| {
                            last_change.elapsed() >= debounce
                        });
                        if ready && !pending.is_empty() {
                            let mut changed = pending.drain().collect::<Vec<_>>();
                            changed.sort();
                            last_change = None;
                            let outcome = hmr
                                .change_many(changed.iter().map(String::as_str))
                                .await;
                            if event_sender.send(WatchEvent { changed, outcome }).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(WatchHandle {
            stop,
            events,
            task: Some(task),
        })
    }

    /// Returns the directly linked module URLs known for a loaded module.
    pub fn linked_dependencies(&self, url: &str) -> Vec<String> {
        let mut dependencies = self
            .modules
            .read()
            .get(url)
            .map(|module| module.dependencies.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        dependencies.sort();
        dependencies
    }

    pub async fn change(&self, url: &str) -> Result<ChangeOutcome> {
        self.change_many([url]).await
    }

    /// Applies a batch of file changes as one replacement transaction.
    pub async fn change_many<'a>(
        &self,
        urls: impl IntoIterator<Item = &'a str>,
    ) -> Result<ChangeOutcome> {
        let _transaction = self.transaction.lock().await;
        let changed = urls.into_iter().map(str::to_owned).collect::<HashSet<_>>();
        let modules = self.modules.read().clone();
        if changed
            .iter()
            .any(|url| modules.get(url).is_some_and(|module| module.external))
        {
            return Ok(ChangeOutcome::FullReload);
        }
        let affected = modules.iter().fold(
            BTreeMap::<String, (String, Option<PluginFactory>)>::new(),
            |mut affected, (entry_url, module)| {
                if let Some(name) = &module.plugin_name {
                    if changed.iter().any(|changed_url| {
                        depends_on(&modules, entry_url, changed_url, &mut HashSet::new())
                    }) {
                        affected
                            .entry(name.clone())
                            .or_insert_with(|| (entry_url.clone(), module.factory.clone()));
                    }
                }
                affected
            },
        );
        if affected.is_empty() {
            for url in changed.iter().filter(|url| !modules.contains_key(*url)) {
                self.loader
                    .context()
                    .emit("hmr/change", vec![shared(url.clone())])
                    .await?;
            }
            return Ok(ChangeOutcome::Ignored);
        }

        // Construct every replacement before mutating the running graph.
        let mut replacements = Vec::with_capacity(affected.len());
        for (name, (module_url, factory)) in affected {
            let factory = factory.ok_or_else(|| {
                CordisError::InvalidConfig(format!("module for `{name}` has no HMR factory"))
            })?;
            replacements.push((name, module_url, factory()?));
        }
        let old = replacements
            .iter()
            .map(|(name, _, _)| {
                self.resolver
                    .get(name)
                    .map(|plugin| (name.clone(), plugin))
                    .ok_or_else(|| {
                        CordisError::InvalidConfig(format!("plugin `{name}` is not registered"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut applied = Vec::new();
        for (name, _, plugin) in &replacements {
            self.resolver.register(name, plugin.clone());
            if let Err(error) = self.loader.reload_name_started(name).await {
                for (old_name, old_plugin) in &old {
                    self.resolver.register(old_name, old_plugin.clone());
                    let _ = self.loader.reload_name(old_name).await;
                }
                return Err(error);
            }
            applied.push(name.clone());
        }
        let reloads = replacements
            .iter()
            .map(|(plugin_name, module_url, _)| HmrReload {
                plugin_name: plugin_name.clone(),
                module_url: module_url.clone(),
            })
            .collect::<Vec<_>>();
        self.loader
            .context()
            .emit("hmr/reload", vec![shared(reloads)])
            .await?;
        Ok(ChangeOutcome::Reloaded(applied))
    }
}

async fn file_stamp(path: &Path) -> Result<FileStamp> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            // Metadata alone can miss equal-length rewrites on coarse-resolution filesystems.
            // Hashing makes each poll O(total watched bytes), favouring correctness for the
            // small source/config files this portable watcher targets.
            let content = tokio::fs::read(path).await.map_err(|error| {
                CordisError::InvalidConfig(format!(
                    "cannot read watched file {}: {error}",
                    path.display()
                ))
            })?;
            let mut hasher = DefaultHasher::new();
            hasher.write(&content);
            Ok(FileStamp {
                modified: metadata.modified().ok(),
                len: metadata.len(),
                content_hash: hasher.finish(),
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(FileStamp {
            modified: None,
            len: 0,
            content_hash: 0,
        }),
        Err(error) => Err(CordisError::InvalidConfig(format!(
            "cannot inspect watched file {}: {error}",
            path.display()
        ))),
    }
}

fn depends_on(
    modules: &HashMap<String, ModuleRecord>,
    current: &str,
    changed: &str,
    visited: &mut HashSet<String>,
) -> bool {
    if current == changed {
        return true;
    }
    if !visited.insert(current.to_owned()) {
        return false;
    }
    modules.get(current).is_some_and(|module| {
        module
            .dependencies
            .iter()
            .any(|dependency| depends_on(modules, dependency, changed, visited))
    })
}

#[cfg(test)]
mod tests {
    use super::file_stamp;

    #[tokio::test]
    async fn file_stamp_distinguishes_equal_length_rewrites() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plugin.rs");
        tokio::fs::write(&path, "first").await.unwrap();
        let first = file_stamp(&path).await.unwrap();
        tokio::fs::write(&path, "other").await.unwrap();
        let other = file_stamp(&path).await.unwrap();

        assert_eq!(first.len, other.len);
        assert_ne!(first.content_hash, other.content_hash);
    }
}
