//! Safe, deterministic Cordis application scaffolding.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct ScaffoldOptions {
    pub destination: PathBuf,
    pub package_name: String,
}

pub fn scaffold(options: &ScaffoldOptions) -> std::io::Result<Vec<PathBuf>> {
    validate_package_name(&options.package_name)?;
    if options.destination.exists() && fs::read_dir(&options.destination)?.next().is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "destination is not empty",
        ));
    }
    fs::create_dir_all(options.destination.join("src"))?;
    let files = [
        (
            PathBuf::from("Cargo.toml"),
            cargo_toml(&options.package_name),
        ),
        (PathBuf::from("src/main.rs"), main_rs()),
        (PathBuf::from("README.md"), readme(&options.package_name)),
        (PathBuf::from(".gitignore"), "/target/\n".to_owned()),
    ];
    let mut created: Vec<PathBuf> = Vec::new();
    for (relative, content) in files {
        let path = options.destination.join(relative);
        if let Err(error) = write_new(&path, &content) {
            for created_file in created.iter().rev() {
                let _ = fs::remove_file(created_file);
            }
            return Err(error);
        }
        created.push(path);
    }
    Ok(created)
}

fn validate_package_name(name: &str) -> std::io::Result<()> {
    let valid = !name.is_empty()
        && name.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || byte == b'_'
                || (byte == b'-' && index > 0)
        });
    if valid {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid Cargo package name",
        ))
    }
}
fn write_new(path: &Path, content: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()
}
fn cargo_toml(name: &str) -> String {
    format!(
        "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\ncordis = {{ version = \"0.1\", features = [\"full\"] }}\ntokio = {{ version = \"1\", features = [\"macros\", \"rt-multi-thread\"] }}\n"
    )
}
fn main_rs() -> String {
    "use cordis::Context;\n\n#[tokio::main]\nasync fn main() -> cordis::Result<()> {\n    let context = Context::new();\n    context.logger().info(\"Cordis started\", vec![])?;\n    Ok(())\n}\n".to_owned()
}
fn readme(name: &str) -> String {
    format!("# {name}\n\nGenerated with `create-cordis`.\n\n```sh\ncargo run\n```\n")
}
