use std::{
    env, fs, io,
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

        if !status.success() {
            return Err(format!("cargo check exited with {status}"));
        }

        Ok(())
    }
}

fn create_clean_dir(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }

    fs::create_dir_all(path)
}
