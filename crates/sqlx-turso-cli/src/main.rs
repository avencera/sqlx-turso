use std::{
    env, fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let command = CommandLine::parse(args)?;

    match command {
        CommandLine::Prepare(prepare) => prepare.run(),
        CommandLine::Help => {
            print_help();
            Ok(())
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum CommandLine {
    Prepare(Prepare),
    Help,
}

#[derive(Debug, Eq, PartialEq)]
struct Prepare {
    database_url: Option<String>,
    offline_dir: PathBuf,
    cargo_args: Vec<String>,
}

impl CommandLine {
    fn parse(args: Vec<String>) -> Result<Self, String> {
        let mut args = args.into_iter();

        match args.next().as_deref() {
            Some("prepare") => Prepare::parse(args.collect()).map(Self::Prepare),
            Some("-h" | "--help") | None => Ok(Self::Help),
            Some(command) => Err(format!("unknown command `{command}`")),
        }
    }
}

impl Prepare {
    fn parse(args: Vec<String>) -> Result<Self, String> {
        let mut database_url = None;
        let mut offline_dir = PathBuf::from(".sqlx");
        let mut cargo_args = Vec::new();
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--" => {
                    cargo_args.extend(args);
                    break;
                }
                "-D" | "--database-url" => {
                    database_url = Some(
                        args.next()
                            .ok_or_else(|| format!("missing value for `{arg}`"))?,
                    );
                }
                "--offline-dir" => {
                    offline_dir = PathBuf::from(
                        args.next()
                            .ok_or_else(|| format!("missing value for `{arg}`"))?,
                    );
                }
                "-h" | "--help" => return Err("use `sqlx-turso --help` for usage".to_owned()),
                option if option.starts_with('-') => {
                    return Err(format!("unknown option `{option}`"));
                }
                value => cargo_args.push(value.to_owned()),
            }
        }

        Ok(Self {
            database_url,
            offline_dir,
            cargo_args,
        })
    }

    fn run(self) -> Result<(), String> {
        create_clean_dir(&self.offline_dir).map_err(|error| {
            format!("failed to prepare {}: {error}", self.offline_dir.display())
        })?;

        let mut command = Command::new(env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned()));
        command.arg("check");

        if self.cargo_args.is_empty() {
            command.arg("--all-targets");
        } else {
            command.args(&self.cargo_args);
        }

        command.env("SQLX_OFFLINE_DIR", &self.offline_dir);
        if let Some(database_url) = self.database_url {
            command.env("DATABASE_URL", database_url);
        }

        let status = command
            .status()
            .map_err(|error| format!("failed to run cargo check: {error}"))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("cargo check exited with {status}"))
        }
    }
}

fn create_clean_dir(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }

    fs::create_dir_all(path)
}

fn print_help() {
    println!(
        "Usage: sqlx-turso prepare [OPTIONS] [-- <CARGO CHECK ARGS>]\n\n\
         Options:\n  -D, --database-url <URL>    database URL used by query macros\n      --offline-dir <DIR>     output directory for query metadata, defaults to .sqlx"
    );
}

#[cfg(test)]
mod tests {
    use super::{CommandLine, Prepare};
    use std::path::PathBuf;

    #[test]
    fn parses_prepare_defaults() {
        assert_eq!(
            CommandLine::parse(vec!["prepare".to_owned()]).unwrap(),
            CommandLine::Prepare(Prepare {
                database_url: None,
                offline_dir: PathBuf::from(".sqlx"),
                cargo_args: Vec::new(),
            })
        );
    }

    #[test]
    fn parses_prepare_options_and_cargo_args() {
        assert_eq!(
            CommandLine::parse(vec![
                "prepare".to_owned(),
                "--database-url".to_owned(),
                "turso::memory:".to_owned(),
                "--offline-dir".to_owned(),
                "target/sqlx".to_owned(),
                "--".to_owned(),
                "-p".to_owned(),
                "sqlx-turso".to_owned(),
                "--features".to_owned(),
                "macros".to_owned(),
            ])
            .unwrap(),
            CommandLine::Prepare(Prepare {
                database_url: Some("turso::memory:".to_owned()),
                offline_dir: PathBuf::from("target/sqlx"),
                cargo_args: vec![
                    "-p".to_owned(),
                    "sqlx-turso".to_owned(),
                    "--features".to_owned(),
                    "macros".to_owned(),
                ],
            })
        );
    }
}
