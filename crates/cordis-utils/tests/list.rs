use std::sync::{Arc, Mutex};

use cordis_core::Context;
use cordis_utils::List;

#[tokio::test]
async fn entries_follow_effect_and_fiber_lifetime() {
    let context = Context::new();
    let list = List::new(context.clone(), "ctx.items");
    let first = list.push(1).await.unwrap();
    list.push(2).await.unwrap();
    assert_eq!(list.values(), vec![1, 2]);
    assert_eq!(list.filter(|value| *value > 1), vec![2]);
    assert_eq!(list.map(|value| value * 10), vec![10, 20]);

    first.dispose().await.unwrap();
    assert_eq!(list.values(), vec![2]);
    context.fiber().dispose().await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
async fn failed_registration_does_not_leak_a_list_entry() {
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
    let list = List::new(context, "ctx.items");

    assert!(list.push(1).await.is_err());
    assert!(list.is_empty());
}
