//! `bloom-tv`: serve the household to a paired Goya TV.
//!
//! ```text
//! bloom-tv serve --bind 192.168.1.129:8780 --state DIR \
//!     --device "AMD Desktop|https://192.168.1.20:7420|aa:bb:cc:dd:ee:ff" \
//!     --device "Fire HD 10" [--token-env BLOOM_API_TOKEN]
//! bloom-tv confirm --state DIR --code 123456
//! ```
//!
//! `serve` prints the pairing code when a TV pairs; `confirm` hands the
//! code read on the TV to the running server.

use bloom_household::{parse_device, Household, HouseholdLink, HttpProbe};
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "usage:\n  bloom-tv serve --bind IP:PORT --state DIR --device \"NAME|URL|MAC\"... [--token-env VAR] [--ca-pem FILE]\n  bloom-tv confirm --state DIR --code NNNNNN";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let (mut bind, mut state, mut code, mut token_env, mut devices) = (None, None, None, None, Vec::new());
    let mut ca_pem = None;
    let mut it = args[1..].iter();
    while let Some(flag) = it.next() {
        let Some(value) = it.next() else {
            eprintln!("{flag} needs a value\n{USAGE}");
            return ExitCode::from(2);
        };
        match flag.as_str() {
            "--bind" => bind = Some(value.clone()),
            "--state" => state = Some(PathBuf::from(value)),
            "--code" => code = Some(value.clone()),
            "--token-env" => token_env = Some(value.clone()),
            "--ca-pem" => ca_pem = Some(PathBuf::from(value)),
            "--device" => match parse_device(value) {
                Ok(d) => devices.push(d),
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::from(2);
                }
            },
            _ => {
                eprintln!("unknown flag {flag}\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(state) = state else {
        eprintln!("--state is required\n{USAGE}");
        return ExitCode::from(2);
    };
    match command.as_str() {
        "confirm" => match goya_lanlink_peer::confirm(&state, code.as_deref().unwrap_or("")) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
        "serve" => {
            let Some(bind) = bind else {
                eprintln!("--bind is required\n{USAGE}");
                return ExitCode::from(2);
            };
            let token = match token_env {
                Some(v) => match std::env::var(&v) {
                    Ok(t) if !t.is_empty() => Some(t),
                    _ => { eprintln!("token environment variable {v} is missing or empty"); return ExitCode::from(2); }
                },
                None => None,
            };
            let ca = match ca_pem.map(std::fs::read).transpose() {
                Ok(ca) => ca,
                Err(e) => { eprintln!("cannot read CA bundle: {e}"); return ExitCode::from(2); }
            };
            let probe = match HttpProbe::new(token, ca.as_deref()) {
                Ok(p) => p,
                Err(e) => { eprintln!("{e}"); return ExitCode::from(2); }
            };
            let household = match Household::new(devices, Box::new(probe)) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::from(2);
                }
            };
            household.spawn_refresher();
            let hint = format!("bloom-tv confirm --state {} --code", state.display());
            let server = match goya_lanlink_peer::Server::new(&state, &hint, HouseholdLink::new(household)) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            match goya_lanlink_peer::serve(&bind, server) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("{e}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}
