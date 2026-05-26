use std::{
    env,
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use clap::{Args, CommandFactory, Parser, Subcommand};

fn main() -> ExitCode {
    match CommandLine::try_parse() {
        Ok(command) => match run(command) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            let exit_code = if error.use_stderr() {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            };

            let _ = error.print();
            exit_code
        }
    }
}

fn run(command: CommandLine) -> Result<(), String> {
    match command.command {
        Some(CommandLineCommand::Prepare(prepare)) => prepare.run(),
        None => {
            let mut command = CommandLine::command();
            command
                .print_help()
                .map_err(|error| format!("failed to print help: {error}"))?;
            println!();
            Ok(())
        }
    }
}

#[derive(Debug, Eq, Parser, PartialEq)]
#[command(version, about)]
struct CommandLine {
    #[command(subcommand)]
    command: Option<CommandLineCommand>,
}

#[derive(Debug, Eq, PartialEq, Subcommand)]
enum CommandLineCommand {
    Prepare(Prepare),
}

#[derive(Args, Debug, Eq, PartialEq)]
struct Prepare {
    #[arg(short = 'D', long)]
    database_url: Option<String>,

    #[arg(long, default_value = ".sqlx")]
    offline_dir: PathBuf,

    #[arg(last = true, value_name = "CARGO CHECK ARGS")]
    cargo_args: Vec<String>,
}

impl Prepare {
    fn run(self) -> Result<(), String> {
        let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        self.run_with_program(cargo)
    }

    fn run_with_program(self, cargo: impl Into<String>) -> Result<(), String> {
        let workspace = PrepareWorkspace::create(&self.offline_dir)?;
        let mut command = self
            .command_spec(cargo, workspace.staging_dir())
            .into_command();

        let status = command
            .status()
            .map_err(|error| format!("failed to run cargo check: {error}"))?;

        if !status.success() {
            workspace.cleanup();
            return Err(format!("cargo check exited with {status}"));
        }

        workspace.commit().map_err(|error| {
            format!(
                "failed to replace {} with prepared metadata: {error}",
                self.offline_dir.display()
            )
        })
    }

    fn command_spec(&self, cargo: impl Into<String>, offline_dir: &Path) -> CommandSpec {
        let mut args = vec!["check".to_owned()];
        if self.cargo_args.is_empty() {
            args.push("--all-targets".to_owned());
        } else {
            args.extend(self.cargo_args.iter().cloned());
        }

        let mut envs = vec![(
            "SQLX_OFFLINE_DIR".to_owned(),
            offline_dir.as_os_str().to_owned(),
        )];
        if let Some(database_url) = &self.database_url {
            envs.push(("DATABASE_URL".to_owned(), OsString::from(database_url)));
        }

        CommandSpec {
            program: cargo.into(),
            args,
            envs,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct CommandSpec {
    program: String,
    args: Vec<String>,
    envs: Vec<(String, OsString)>,
}

impl CommandSpec {
    fn into_command(self) -> Command {
        let mut command = Command::new(self.program);
        command.args(self.args);
        for (key, value) in self.envs {
            command.env(key, value);
        }

        command
    }
}

#[derive(Debug)]
struct PrepareWorkspace {
    final_dir: PathBuf,
    staging_dir: PathBuf,
    backup_dir: PathBuf,
}

impl PrepareWorkspace {
    fn create(final_dir: &Path) -> Result<Self, String> {
        validate_offline_dir(final_dir)?;

        if final_dir.exists() && !final_dir.is_dir() {
            return Err(format!("{} is not a directory", final_dir.display()));
        }

        let parent = final_dir.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create parent directory {}: {error}",
                parent.display()
            )
        })?;

        let staging_dir = unique_sibling(final_dir, "tmp");
        fs::create_dir(&staging_dir).map_err(|error| {
            format!(
                "failed to create staging directory {}: {error}",
                staging_dir.display()
            )
        })?;

        Ok(Self {
            final_dir: final_dir.to_path_buf(),
            staging_dir,
            backup_dir: unique_sibling(final_dir, "old"),
        })
    }

    fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    fn commit(self) -> io::Result<()> {
        if self.backup_dir.exists() {
            fs::remove_dir_all(&self.backup_dir)?;
        }

        let had_existing = self.final_dir.exists();
        if had_existing {
            fs::rename(&self.final_dir, &self.backup_dir)?;
        }

        match fs::rename(&self.staging_dir, &self.final_dir) {
            Ok(()) => {
                if had_existing {
                    fs::remove_dir_all(&self.backup_dir)?;
                }
                Ok(())
            }
            Err(error) => {
                if had_existing {
                    let _ = fs::rename(&self.backup_dir, &self.final_dir);
                }

                Err(error)
            }
        }
    }

    fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.staging_dir);
    }
}

fn validate_offline_dir(path: &Path) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err("offline directory cannot be empty".to_owned());
    }

    if path == Path::new(".") {
        return Err("offline directory cannot be the current directory".to_owned());
    }

    if path.parent().is_none() && path.has_root() {
        return Err("offline directory cannot be the filesystem root".to_owned());
    }

    Ok(())
}

fn unique_sibling(path: &Path, suffix: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("sqlx"))
        .to_string_lossy();

    let pid = std::process::id();

    for attempt in 0.. {
        let candidate = parent.join(format!(".{name}.{suffix}-{pid}-{attempt}"));
        if !candidate.exists() {
            return candidate;
        }
    }

    unreachable!("unbounded suffix search must return a candidate")
}

#[cfg(test)]
mod tests {
    use std::{fs, process};

    use super::{CommandSpec, Prepare, PrepareWorkspace};

    #[test]
    fn command_spec_uses_default_check_args_and_staging_dir() {
        let prepare = Prepare {
            database_url: Some("turso::memory:".to_owned()),
            offline_dir: ".sqlx".into(),
            cargo_args: Vec::new(),
        };

        let spec = prepare.command_spec("cargo-test", "target/offline-staging".as_ref());

        assert_eq!(
            spec,
            CommandSpec {
                program: "cargo-test".to_owned(),
                args: vec!["check".to_owned(), "--all-targets".to_owned()],
                envs: vec![
                    (
                        "SQLX_OFFLINE_DIR".to_owned(),
                        "target/offline-staging".into()
                    ),
                    ("DATABASE_URL".to_owned(), "turso::memory:".into()),
                ],
            }
        );
    }

    #[test]
    fn workspace_keeps_existing_dir_until_commit() {
        let root = std::env::temp_dir().join(format!(
            "sqlx-turso-cli-{}-{}",
            process::id(),
            "workspace-keeps-existing"
        ));
        let final_dir = root.join(".sqlx");
        let old_file = final_dir.join("old.json");
        let new_file = "new.json";

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&final_dir).unwrap();
        fs::write(&old_file, b"old").unwrap();

        let workspace = PrepareWorkspace::create(&final_dir).unwrap();
        fs::write(workspace.staging_dir().join(new_file), b"new").unwrap();

        assert!(old_file.exists());
        workspace.commit().unwrap();

        assert!(!old_file.exists());
        assert!(final_dir.join(new_file).exists());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn workspace_cleanup_preserves_existing_dir() {
        let root = std::env::temp_dir().join(format!(
            "sqlx-turso-cli-{}-{}",
            process::id(),
            "workspace-cleanup"
        ));
        let final_dir = root.join(".sqlx");
        let old_file = final_dir.join("old.json");

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&final_dir).unwrap();
        fs::write(&old_file, b"old").unwrap();

        let workspace = PrepareWorkspace::create(&final_dir).unwrap();
        let staging_dir = workspace.staging_dir().to_path_buf();
        workspace.cleanup();

        assert!(old_file.exists());
        assert!(!staging_dir.exists());

        let _ = fs::remove_dir_all(&root);
    }
}
