//! Command-line parsing (1.5): every value is a long option, and a missing or
//! positional value is an error.

use std::path::PathBuf;

use brisk::cli::{Cli, Command};
use clap::error::ErrorKind;
use clap::{CommandFactory as _, Parser as _};

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("brisk").chain(args.iter().copied()))
}

fn parse_err(args: &[&str]) -> ErrorKind {
    match parse(args) {
        Ok(cli) => panic!("{args:?} parsed as {cli:?}"),
        Err(err) => err.kind(),
    }
}

#[test]
fn definition_is_consistent() {
    Cli::command().debug_assert();
}

#[test]
fn serve_and_check_config_take_config() {
    assert_eq!(
        parse(&["serve", "--config", "cpa.local.toml"])
            .unwrap()
            .command,
        Command::Serve {
            config: PathBuf::from("cpa.local.toml")
        }
    );
    assert_eq!(
        parse(&["check-config", "--config=/etc/brisk/brisk.toml"])
            .unwrap()
            .command,
        Command::CheckConfig {
            config: PathBuf::from("/etc/brisk/brisk.toml")
        }
    );
}

#[test]
fn keygen_takes_name() {
    assert_eq!(
        parse(&["keygen", "--name", "local-dev"]).unwrap().command,
        Command::Keygen {
            name: String::from("local-dev")
        }
    );
    // The name is checked by `keygen::generate`, not by the parser.
    assert_eq!(
        parse(&["keygen", "--name", ""]).unwrap().command,
        Command::Keygen {
            name: String::new()
        }
    );
}

#[test]
fn missing_values_are_errors() {
    for args in [&["serve"][..], &["check-config"], &["keygen"]] {
        assert_eq!(
            parse_err(args),
            ErrorKind::MissingRequiredArgument,
            "{args:?}"
        );
    }
    for args in [&["serve", "--config"][..], &["keygen", "--name"]] {
        assert_eq!(parse_err(args), ErrorKind::InvalidValue, "{args:?}");
    }
    // Each subcommand takes only its own option.
    assert_eq!(
        parse_err(&["serve", "--name", "x"]),
        ErrorKind::UnknownArgument
    );
    assert_eq!(
        parse_err(&["keygen", "--config", "brisk.toml"]),
        ErrorKind::UnknownArgument
    );
}

#[test]
fn positional_values_are_rejected() {
    assert_eq!(
        parse_err(&["serve", "cpa.local.toml"]),
        ErrorKind::UnknownArgument
    );
    assert_eq!(
        parse_err(&["check-config", "cpa.local.toml"]),
        ErrorKind::UnknownArgument
    );
    assert_eq!(
        parse_err(&["keygen", "local-dev"]),
        ErrorKind::UnknownArgument
    );
}

#[test]
fn a_subcommand_is_required() {
    assert_eq!(
        parse_err(&[]),
        ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
    );
    assert_eq!(parse_err(&["run"]), ErrorKind::InvalidSubcommand);
}
