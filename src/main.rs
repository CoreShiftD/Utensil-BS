// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/

use utensil_bs::{cmd_status, cmd_stop, cmd_uevent_dump, cmd_watch, run_supervisor};
use coreshift_core::unix_socket::{connect_unix_stream, UnixSocketAddr};
use std::time::{Duration, Instant};

fn print_usage() {
    println!("Usage: utensil-bs <command>");
    println!("Commands:");
    println!("  daemon       Start the battery-status daemon (supervised, detached)");
    println!("  stop         Stop the running daemon");
    println!("  restart      Stop then start the daemon");
    println!("  status       Print current battery level");
    println!("  watch        Stream battery level changes to stdout");
    println!("  uevent-dump  Dump raw kernel uevents (for debugging)");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 { print_usage(); return; }

    let result: Result<(), Box<dyn std::error::Error>> = match args[1].as_str() {
        "daemon" => run_supervisor(),
        "stop"   => cmd_stop(),
        "restart" => {
            cmd_stop().ok();
            let addr = UnixSocketAddr::Abstract(utensil_bs::SOCKET);
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(3) {
                if connect_unix_stream(addr).is_err() { break; }
                std::thread::sleep(Duration::from_millis(100));
            }
            run_supervisor()
        }
        "status"      => cmd_status(),
        "watch"       => cmd_watch(),
        "uevent-dump" => cmd_uevent_dump(),
        _             => { print_usage(); return; }
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
