use cordis_create::{ScaffoldOptions, scaffold};

#[test]
fn creates_complete_project_and_refuses_existing_content() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("sample-app");
    let files = scaffold(&ScaffoldOptions {
        destination: destination.clone(),
        package_name: "sample-app".into(),
    })
    .unwrap();
    assert_eq!(files.len(), 4);
    assert!(destination.join("Cargo.toml").exists());
    assert!(
        std::fs::read_to_string(destination.join("src/main.rs"))
            .unwrap()
            .contains("Context::new")
    );
    assert_eq!(
        scaffold(&ScaffoldOptions {
            destination,
            package_name: "sample-app".into()
        })
        .unwrap_err()
        .kind(),
        std::io::ErrorKind::AlreadyExists
    );
}

#[test]
fn rejects_unsafe_package_names_without_writing() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("bad");
    assert!(
        scaffold(&ScaffoldOptions {
            destination: destination.clone(),
            package_name: "../bad".into()
        })
        .is_err()
    );
    assert!(!destination.exists());
}
