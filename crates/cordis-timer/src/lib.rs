//! Fiber-scoped timeout, interval, throttle, and debounce primitives.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use cordis_core::{Context, CordisError, Disposer, EffectHandle, Result};
use parking_lot::Mutex;
use tokio::time::Instant;

#[derive(Clone)]
pub struct TimerService {
    context: Context,
}

impl TimerService {
    pub fn new(context: Context) -> Self {
        Self { context }
    }

    pub async fn timeout<F>(&self, delay: Duration, callback: F) -> Result<EffectHandle>
    where
        F: FnOnce() + Send + 'static,
    {
        let (effect_sender, effect_receiver) = tokio::sync::oneshot::channel::<EffectHandle>();
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = Arc::clone(&completed);
        let effect = self
            .context
            .effect("ctx.timeout()", move |scope| async move {
                let task = tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let Ok(effect) = effect_receiver.await else {
                        return;
                    };
                    task_completed.store(true, Ordering::Release);
                    let _ = effect.dispose().await;
                    callback();
                });
                let abort = task.abort_handle();
                let disposer: Disposer = Arc::new(move || {
                    if !completed.load(Ordering::Acquire) {
                        abort.abort();
                    }
                    Box::pin(async { Ok(()) })
                });
                scope.defer(disposer);
                Ok(None)
            })
            .await?;
        let _ = effect_sender.send(effect.clone());
        Ok(effect)
    }

    #[deprecated(note = "use TimerService::timeout")]
    pub async fn set_timeout<F>(&self, delay: Duration, callback: F) -> Result<EffectHandle>
    where
        F: FnOnce() + Send + 'static,
    {
        self.timeout(delay, callback).await
    }

    pub async fn sleep(&self, delay: Duration) -> Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let (effect, _) = self
            .task_effect("ctx.timeout()", move || {
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = sender.send(());
                })
            })
            .await?;
        let result = receiver.await;
        effect.dispose().await?;
        result.map_err(|_| CordisError::InactiveEffect)
    }

    pub async fn interval<F>(&self, delay: Duration, callback: F) -> Result<EffectHandle>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);
        self.task_effect("ctx.interval()", move || {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    callback();
                }
            })
        })
        .await
        .map(|(effect, _)| effect)
    }

    #[deprecated(note = "use TimerService::interval")]
    pub async fn set_interval<F>(&self, delay: Duration, callback: F) -> Result<EffectHandle>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.interval(delay, callback).await
    }

    pub async fn interval_stream(&self, delay: Duration) -> Result<Interval> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (effect, abort) = self
            .task_effect("ctx.interval()", move || {
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(delay);
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        if sender.send(()).await.is_err() {
                            break;
                        }
                    }
                })
            })
            .await?;
        Ok(Interval {
            receiver,
            effect,
            abort,
            manual: Arc::new(AtomicBool::new(false)),
            reported_closed: false,
        })
    }

    pub async fn debounce<T, F>(&self, delay: Duration, callback: F) -> Result<Scheduled<T>>
    where
        T: Send + 'static,
        F: Fn(T) + Send + Sync + 'static,
    {
        Scheduled::new(
            self.context.clone(),
            delay,
            callback,
            ScheduleMode::Debounce,
        )
        .await
    }

    pub async fn throttle<T, F>(
        &self,
        delay: Duration,
        no_trailing: bool,
        callback: F,
    ) -> Result<Scheduled<T>>
    where
        T: Send + 'static,
        F: Fn(T) + Send + Sync + 'static,
    {
        Scheduled::new(
            self.context.clone(),
            delay,
            callback,
            ScheduleMode::Throttle { no_trailing },
        )
        .await
    }

    async fn task_effect<F>(
        &self,
        label: &'static str,
        start: F,
    ) -> Result<(EffectHandle, tokio::task::AbortHandle)>
    where
        F: FnOnce() -> tokio::task::JoinHandle<()> + Send + 'static,
    {
        let abort_slot = Arc::new(Mutex::new(None));
        let setup_slot = Arc::clone(&abort_slot);
        let effect = self
            .context
            .effect(label, move |scope| async move {
                let abort = start().abort_handle();
                *setup_slot.lock() = Some(abort.clone());
                let disposer: Disposer = Arc::new(move || {
                    abort.abort();
                    Box::pin(async { Ok(()) })
                });
                scope.defer(disposer);
                Ok(None)
            })
            .await?;
        let abort = abort_slot
            .lock()
            .take()
            .ok_or_else(|| CordisError::Internal("timer task was not started".to_owned()))?;
        Ok((effect, abort))
    }
}

pub struct Interval {
    receiver: tokio::sync::mpsc::Receiver<()>,
    effect: EffectHandle,
    abort: tokio::task::AbortHandle,
    manual: Arc<AtomicBool>,
    reported_closed: bool,
}

impl Drop for Interval {
    fn drop(&mut self) {
        self.manual.store(true, Ordering::Release);
        self.abort.abort();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let effect = self.effect.clone();
            runtime.spawn(async move {
                let _ = effect.dispose().await;
            });
        }
    }
}

impl Interval {
    pub async fn tick(&mut self) -> Result<()> {
        self.receiver
            .recv()
            .await
            .ok_or(CordisError::InactiveEffect)
    }

    pub async fn dispose(&self) -> Result<()> {
        self.manual.store(true, Ordering::Release);
        self.effect.dispose().await
    }
}

impl futures::Stream for Interval {
    type Item = Result<()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(())) => Poll::Ready(Some(Ok(()))),
            Poll::Ready(None) if !self.manual.load(Ordering::Acquire) && !self.reported_closed => {
                self.reported_closed = true;
                Poll::Ready(Some(Err(CordisError::InactiveEffect)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ScheduleMode {
    Debounce,
    Throttle { no_trailing: bool },
}

struct ScheduleState {
    last_call: Option<Instant>,
    pending: Option<tokio::task::AbortHandle>,
}

pub struct Scheduled<T> {
    delay: Duration,
    mode: ScheduleMode,
    callback: Arc<dyn Fn(T) + Send + Sync>,
    state: Arc<Mutex<ScheduleState>>,
    disposed: Arc<AtomicBool>,
    effect: EffectHandle,
}

impl<T> Scheduled<T>
where
    T: Send + 'static,
{
    async fn new<F>(
        context: Context,
        delay: Duration,
        callback: F,
        mode: ScheduleMode,
    ) -> Result<Self>
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        let state = Arc::new(Mutex::new(ScheduleState {
            last_call: None,
            pending: None,
        }));
        let disposed = Arc::new(AtomicBool::new(false));
        let dispose_state = Arc::clone(&state);
        let dispose_flag = Arc::clone(&disposed);
        let label = match mode {
            ScheduleMode::Debounce => "ctx.debounce()",
            ScheduleMode::Throttle { .. } => "ctx.throttle()",
        };
        let effect = context
            .effect(label, move |scope| async move {
                let disposer: Disposer = Arc::new(move || {
                    dispose_flag.store(true, Ordering::Release);
                    if let Some(task) = dispose_state.lock().pending.take() {
                        task.abort();
                    }
                    Box::pin(async { Ok(()) })
                });
                scope.defer(disposer);
                Ok(None)
            })
            .await?;
        Ok(Self {
            delay,
            mode,
            callback: Arc::new(callback),
            state,
            disposed,
            effect,
        })
    }

    pub fn call(&self, value: T) {
        let now = Instant::now();
        let mut state = self.state.lock();
        if let Some(task) = state.pending.take() {
            task.abort();
        }
        match self.mode {
            ScheduleMode::Debounce => {
                if self.disposed.load(Ordering::Acquire) {
                    return;
                }
                state.pending = Some(spawn_delayed(self.delay, Arc::clone(&self.callback), value));
            }
            ScheduleMode::Throttle { no_trailing } => {
                let remaining = state
                    .last_call
                    .and_then(|last| self.delay.checked_sub(now.saturating_duration_since(last)));
                match remaining {
                    None => {
                        state.last_call = Some(now);
                        drop(state);
                        (self.callback)(value);
                    }
                    Some(delay) if !no_trailing && !self.disposed.load(Ordering::Acquire) => {
                        let callback = Arc::clone(&self.callback);
                        let state_ref = Arc::clone(&self.state);
                        let task = tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            state_ref.lock().last_call = Some(Instant::now());
                            callback(value);
                        });
                        state.pending = Some(task.abort_handle());
                    }
                    Some(_) => {}
                }
            }
        }
    }

    pub async fn dispose(&self) -> Result<()> {
        self.effect.dispose().await
    }
}

fn spawn_delayed<T>(
    delay: Duration,
    callback: Arc<dyn Fn(T) + Send + Sync>,
    value: T,
) -> tokio::task::AbortHandle
where
    T: Send + 'static,
{
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        callback(value);
    })
    .abort_handle()
}
