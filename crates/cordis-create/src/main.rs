use cordis_create::{ScaffoldOptions, scaffold};
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let destination = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cordis-app"));
    let package_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cordis-app")
        .to_owned();
    let files = scaffold(&ScaffoldOptions {
        destination,
        package_name,
    })?;
    println!("created {} files", files.len());
    Ok(())
}
