use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use cordis_core::{Context, CordisError};
use cordis_timer::TimerService;
use futures::StreamExt;

#[tokio::test(start_paused = true)]
async fn timeout_interval_and_disposal_follow_fiber() {
    let context = Context::new();
    let timer = TimerService::new(context.clone());
    let timeouts = Arc::new(AtomicUsize::new(0));
    let timeout_count = Arc::clone(&timeouts);
    timer
        .timeout(Duration::from_secs(1), move || {
            timeout_count.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(timeouts.load(Ordering::SeqCst), 1);

    let intervals = Arc::new(AtomicUsize::new(0));
    let interval_count = Arc::clone(&intervals);
    timer
        .interval(Duration::from_secs(1), move || {
            interval_count.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(intervals.load(Ordering::SeqCst) >= 1);
    context.fiber().dispose().await.unwrap();
    let stopped = intervals.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(intervals.load(Ordering::SeqCst), stopped);
}

#[tokio::test(start_paused = true)]
async fn debounce_and_throttle_keep_latest_expected_call() {
    let context = Context::new();
    let timer = TimerService::new(context);
    let values = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let debounce_values = Arc::clone(&values);
    let debounce = timer
        .debounce(Duration::from_secs(1), move |value| {
            debounce_values.lock().push(value);
        })
        .await
        .unwrap();
    debounce.call(1);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(500)).await;
    debounce.call(2);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(&*values.lock(), &[2]);

    let throttle_values = Arc::clone(&values);
    let throttle = timer
        .throttle(Duration::from_secs(1), false, move |value| {
            throttle_values.lock().push(value);
        })
        .await
        .unwrap();
    throttle.call(3);
    throttle.call(4);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(&*values.lock(), &[2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn timeout_can_be_cancelled_before_it_fires() {
    let context = Context::new();
    let timer = TimerService::new(context);
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let timeout = timer
        .timeout(Duration::from_secs(1), move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();

    timeout.dispose().await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn fired_timeout_unregisters_its_effect_before_callback() {
    let context = Context::new();
    let timer = TimerService::new(context.clone());
    let fiber = context.fiber();
    let callback_fiber = fiber.clone();
    let effect_was_gone = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&effect_was_gone);
    timer
        .timeout(Duration::from_secs(1), move || {
            observed.store(callback_fiber.get_effects().is_empty(), Ordering::SeqCst);
        })
        .await
        .unwrap();
    assert_eq!(fiber.get_effects().len(), 1);

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(effect_was_gone.load(Ordering::SeqCst));
    assert!(fiber.get_effects().is_empty());
}

#[tokio::test(start_paused = true)]
async fn sleep_resolves_at_deadline_and_fails_when_its_context_is_disposed() {
    let context = Context::new();
    let timer = TimerService::new(context.clone());
    let sleeper = tokio::spawn({
        let timer = timer.clone();
        async move { timer.sleep(Duration::from_secs(1)).await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(sleeper.await.unwrap().is_ok());

    let cancelled = tokio::spawn(async move { timer.sleep(Duration::from_secs(1)).await });
    tokio::task::yield_now().await;
    context.fiber().dispose().await.unwrap();
    tokio::task::yield_now().await;
    assert!(matches!(
        cancelled.await.unwrap(),
        Err(CordisError::InactiveEffect)
    ));
}

#[tokio::test(start_paused = true)]
async fn interval_stream_ticks_and_manual_disposal_closes_it() {
    let context = Context::new();
    let timer = TimerService::new(context);
    let mut interval = timer.interval_stream(Duration::from_secs(1)).await.unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(interval.tick().await.is_ok());
    interval.dispose().await.unwrap();
    tokio::task::yield_now().await;
    assert!(matches!(
        interval.tick().await,
        Err(CordisError::InactiveEffect)
    ));
}

#[tokio::test(start_paused = true)]
async fn interval_stream_closes_when_its_context_is_disposed() {
    let context = Context::new();
    let timer = TimerService::new(context.clone());
    let mut interval = timer.interval_stream(Duration::from_secs(1)).await.unwrap();
    tokio::task::yield_now().await;
    context.fiber().dispose().await.unwrap();
    tokio::task::yield_now().await;

    assert!(matches!(
        interval.tick().await,
        Err(CordisError::InactiveEffect)
    ));
}

#[tokio::test(start_paused = true)]
async fn dropping_interval_stream_unregisters_its_effect() {
    let context = Context::new();
    let fiber = context.fiber();
    let timer = TimerService::new(context);
    let interval = timer.interval_stream(Duration::from_secs(1)).await.unwrap();
    assert_eq!(fiber.get_effects().len(), 1);

    drop(interval);
    tokio::task::yield_now().await;
    assert!(fiber.get_effects().is_empty());
}

#[tokio::test(start_paused = true)]
async fn interval_implements_stream_return_and_context_error_semantics() {
    let manual_context = Context::new();
    let mut manual = TimerService::new(manual_context)
        .interval_stream(Duration::from_secs(1))
        .await
        .unwrap();
    manual.dispose().await.unwrap();
    assert!(manual.next().await.is_none());

    let disposed_context = Context::new();
    let mut disposed = TimerService::new(disposed_context.clone())
        .interval_stream(Duration::from_secs(1))
        .await
        .unwrap();
    disposed_context.fiber().dispose().await.unwrap();
    assert!(matches!(
        disposed.next().await,
        Some(Err(CordisError::InactiveEffect))
    ));
    assert!(disposed.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn throttle_supports_trailing_and_no_trailing_modes() {
    let context = Context::new();
    let timer = TimerService::new(context);
    let trailing_values = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let capture = Arc::clone(&trailing_values);
    let trailing = timer
        .throttle(Duration::from_secs(1), false, move |value| {
            capture.lock().push(value);
        })
        .await
        .unwrap();
    trailing.call(1);
    tokio::time::advance(Duration::from_millis(500)).await;
    trailing.call(2);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    assert_eq!(&*trailing_values.lock(), &[1, 2]);

    let leading_values = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let capture = Arc::clone(&leading_values);
    let leading = timer
        .throttle(Duration::from_secs(1), true, move |value| {
            capture.lock().push(value);
        })
        .await
        .unwrap();
    leading.call(1);
    tokio::time::advance(Duration::from_millis(500)).await;
    leading.call(2);
    tokio::time::advance(Duration::from_millis(501)).await;
    leading.call(3);
    tokio::task::yield_now().await;
    assert_eq!(&*leading_values.lock(), &[1, 3]);
}

#[tokio::test(start_paused = true)]
async fn disposed_schedulers_match_upstream_call_semantics() {
    let context = Context::new();
    let timer = TimerService::new(context);
    let debounce_calls = Arc::new(AtomicUsize::new(0));
    let capture = Arc::clone(&debounce_calls);
    let debounce = timer
        .debounce(Duration::from_secs(1), move |()| {
            capture.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
    debounce.dispose().await.unwrap();
    debounce.call(());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(debounce_calls.load(Ordering::SeqCst), 0);

    let throttle_calls = Arc::new(AtomicUsize::new(0));
    let capture = Arc::clone(&throttle_calls);
    let throttle = timer
        .throttle(Duration::from_secs(1), false, move |()| {
            capture.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
    throttle.dispose().await.unwrap();
    throttle.call(());
    tokio::time::advance(Duration::from_millis(500)).await;
    throttle.call(());
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(throttle_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn inactive_context_does_not_spawn_a_timer_before_registration_fails() {
    let root = Context::new();
    let captured = Arc::new(Mutex::new(None));
    let callback = {
        let captured = Arc::clone(&captured);
        move |context| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock().unwrap() = Some(context);
                Ok(None)
            }
        }
    };
    let fiber = root
        .inject(std::iter::empty::<&str>(), callback)
        .await
        .unwrap();
    let context = captured.lock().unwrap().take().unwrap();
    fiber.dispose().await.unwrap();
    let timer = TimerService::new(context);
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);

    assert!(
        timer
            .timeout(Duration::from_secs(1), move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .is_err()
    );
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
