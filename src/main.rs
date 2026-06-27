// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/

use coreshift_core::android_property::{android_property_get, android_property_set};
use coreshift_core::inotify;
use coreshift_core::netlink;
use coreshift_core::process::{
    close_fds_from, fork, redirect_fd_to, redirect_stdio_to_devnull, set_pdeathsig, setsid,
    setpgid, ForkResult,
};
use coreshift_core::reactor::{Event, Fd, Reactor, Token};
use coreshift_core::signal::{signal_ignore, SIGHUP, SIGPIPE, SIGTERM};
use coreshift_core::spawn::{ExitStatus, Process};
use coreshift_core::unix_socket::{
    self, connect_unix_stream, connect_unix_stream_named, UnixConnectResult, UnixSocketAddr,
    UnixSocketBindOptions, UnixStreamFd,
};
use coreshift_core::{log_error, log_info, log_warn};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

const TAG:            &str = "utensil-bs";
const SOCKET:         &[u8] = b"coreshift_batterystats";
const SOCKET_WATCHER: &[u8] = b"coreshift_bs_consumer";
const SYSFS_CAP:      &str  = "/sys/class/power_supply/battery/capacity";
const SYSFS_STATUS:   &str  = "/sys/class/power_supply/battery/status";
const CHARGE_PROP:    &str  = "debug.tracing.charge_state";
const DATA_DIR:       &str  = "/data/local/tmp/Utensil";
const PID_FILE:       &str  = "/data/local/tmp/Utensil/bs.pid";
const LOG_FILE:       &str  = "/data/local/tmp/Utensil/bs.log";

// ── battery event source ──────────────────────────────────────────────────────

enum BatterySource {
    /// inotify on sysfs capacity + status files. Event means: re-read sysfs.
    Inotify(Fd),
    /// NETLINK_KOBJECT_UEVENT. Event drains messages and returns parsed values.
    Uevent(Fd),
}

impl BatterySource {
    /// Try inotify first; fall back to uevent on EPERM.
    fn open() -> Result<Self, coreshift_core::CoreError> {
        match inotify::init() {
            Ok(fd) => {
                let cap_ok = inotify::add_watch(&fd, SYSFS_CAP, inotify::MODIFY_MASK).is_ok();
                let _      = inotify::add_watch(&fd, SYSFS_STATUS, inotify::MODIFY_MASK);
                if cap_ok {
                    return Ok(BatterySource::Inotify(fd));
                }
                // inotify init succeeded but watch failed (sysfs ACL) — try uevent.
            }
            Err(_) => {}
        }
        netlink::uevent_open().map(BatterySource::Uevent)
    }

    fn fd(&self) -> &Fd {
        match self {
            BatterySource::Inotify(fd) | BatterySource::Uevent(fd) => fd,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            BatterySource::Inotify(_) => "inotify",
            BatterySource::Uevent(_)  => "uevent",
        }
    }

    /// Called when the reactor fires for this source.
    /// Returns `(level, status)` if a meaningful change was detected.
    fn drain(&self) -> Option<(u8, String)> {
        match self {
            BatterySource::Inotify(fd) => {
                // Drain all pending inotify events (EPOLLET).
                let _ = inotify::read_events(fd);
                let cap    = read_level()?;
                let status = read_status();
                Some((cap, status))
            }
            BatterySource::Uevent(fd) => {
                let (msg_cap, status) = netlink::uevent_drain_battery(fd)?;
                let cap = msg_cap.or_else(read_level)?;
                Some((cap, status))
            }
        }
    }
}

// ── sysfs helpers ─────────────────────────────────────────────────────────────

fn read_level() -> Option<u8> {
    std::fs::read_to_string(SYSFS_CAP).ok()?.trim().parse().ok()
}

fn read_status() -> String {
    std::fs::read_to_string(SYSFS_STATUS)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn status_str(raw: &str) -> &'static str {
    match raw {
        "2" => "Charging",
        "3" => "Discharging",
        "4" => "Not charging",
        "5" => "Full",
        _   => "Unknown",
    }
}

// ── broadcast ─────────────────────────────────────────────────────────────────

fn broadcast(watchers: &mut HashMap<Token, UnixStreamFd>, msg: &[u8], reactor: &mut Reactor) {
    let dead: Vec<Token> = watchers
        .iter()
        .filter_map(|(&tok, stream)| {
            if stream.fd.write_slice(msg).is_err() { Some(tok) } else { None }
        })
        .collect();
    for tok in dead {
        if let Some(s) = watchers.remove(&tok) { let _ = reactor.del(&s.fd); }
    }
}

// ── socket helpers ────────────────────────────────────────────────────────────

fn connect() -> Result<UnixStreamFd, Box<dyn std::error::Error>> {
    match connect_unix_stream(UnixSocketAddr::Abstract(SOCKET))? {
        UnixConnectResult::Connected(s) => Ok(s),
        UnixConnectResult::InProgress(_) => Err("connection in progress".into()),
    }
}

fn connect_as_consumer() -> Result<UnixStreamFd, Box<dyn std::error::Error>> {
    match connect_unix_stream_named(
        UnixSocketAddr::Abstract(SOCKET),
        UnixSocketAddr::Abstract(SOCKET_WATCHER),
    )? {
        UnixConnectResult::Connected(s) => Ok(s),
        UnixConnectResult::InProgress(_) => Err("connection in progress".into()),
    }
}

// ── subcommands ───────────────────────────────────────────────────────────────

fn cmd_watch() -> Result<(), Box<dyn std::error::Error>> {
    let stream = connect_as_consumer().map_err(|_| "daemon not running")?;
    let mut reactor = Reactor::new()?;
    let tok = reactor.add(&stream.fd, true, false)?;
    let mut events = Vec::new();
    loop {
        reactor.wait(&mut events, 1, -1)?;
        for ev in &events {
            if ev.token != tok { continue; }
            let mut buf = [0u8; 64];
            match stream.fd.read_slice(&mut buf)? {
                Some(0) | None => return Ok(()),
                Some(n) => io::stdout().write_all(&buf[..n])?,
            }
        }
    }
}

fn cmd_uevent_dump() -> Result<(), Box<dyn std::error::Error>> {
    let fd = netlink::uevent_open()?;
    eprintln!("listening for uevents (Ctrl-C to stop)...");
    let mut buf = [0u8; 4096];
    loop {
        let n = loop {
            if let Some(n) = netlink::uevent_recv(&fd, &mut buf) { break n; }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let fields: Vec<&str> = buf[..n]
            .split(|&b| b == 0)
            .filter_map(|s| std::str::from_utf8(s).ok())
            .filter(|s| !s.is_empty())
            .collect();
        println!("--- uevent ({n}b) ---");
        for f in &fields { println!("  {f}"); }
    }
}

fn cmd_status() -> Result<(), Box<dyn std::error::Error>> {
    let stream = connect().map_err(|_| "daemon not running")?;
    let mut buf = [0u8; 64];
    match stream.fd.read_slice(&mut buf)? {
        Some(n) if n > 0 => io::stdout().write_all(&buf[..n])?,
        _ => println!("no data"),
    }
    Ok(())
}

fn cmd_stop() -> Result<(), Box<dyn std::error::Error>> {
    let pid_file = Path::new(PID_FILE);
    match std::fs::read_to_string(pid_file) {
        Err(_) => { println!("daemon not running (no PID file)"); return Ok(()); }
        Ok(s)  => {
            let pid: i32 = s.trim().parse()?;
            let _ = Process::new(pid).kill(SIGTERM);
            let start = Instant::now();
            while pid_file.exists() && start.elapsed() < Duration::from_secs(3) {
                std::thread::sleep(Duration::from_millis(100));
            }
            if pid_file.exists() {
                println!("warning: PID file still exists after SIGTERM");
            } else {
                println!("daemon stopped");
            }
        }
    }
    Ok(())
}

fn run_supervisor() -> Result<(), Box<dyn std::error::Error>> {
    let addr = UnixSocketAddr::Abstract(SOCKET);
    if connect_unix_stream(addr).is_ok() {
        println!("daemon already running");
        return Ok(());
    }

    let _ = std::fs::create_dir_all(DATA_DIR);

    match unsafe { fork()? } {
        ForkResult::Parent(pid) => {
            let _ = Process::new(pid).wait_blocking();
            let start = Instant::now();
            loop {
                if connect_unix_stream(addr).is_ok() { break; }
                if start.elapsed() > Duration::from_secs(10) {
                    println!("warning: daemon start timed out");
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            return Ok(());
        }
        ForkResult::Child => {}
    }

    let _ = setsid();
    let _ = setpgid(0, 0);

    match unsafe { fork()? } {
        ForkResult::Parent(_) => std::process::exit(0),
        ForkResult::Child => {}
    }

    unsafe { let _ = redirect_stdio_to_devnull(); }

    let pid_file = Path::new(PID_FILE);
    let mut crash_count: u64 = 0;
    let mut last_crash_window = Instant::now();

    loop {
        match unsafe { fork()? } {
            ForkResult::Parent(daemon_pid) => {
                let _ = std::fs::write(pid_file, daemon_pid.to_string());
                let process = Process::new(daemon_pid);
                let status = process.wait_blocking();
                let _ = std::fs::remove_file(pid_file);

                if let Ok(ExitStatus::Exited(0)) = status {
                    std::process::exit(0);
                }

                crash_count += 1;
                if last_crash_window.elapsed() > Duration::from_secs(10) {
                    crash_count = 1;
                    last_crash_window = Instant::now();
                }
                if crash_count >= 5 {
                    std::process::exit(1);
                }
                std::thread::sleep(Duration::from_millis(500 * crash_count));
            }
            ForkResult::Child => {
                let _ = set_pdeathsig(SIGTERM);
                unsafe {
                    signal_ignore(SIGHUP);
                    signal_ignore(SIGPIPE);
                }
                close_fds_from(3);

                if let Ok(f) = std::fs::OpenOptions::new()
                    .create(true).append(true).open(LOG_FILE)
                {
                    use std::os::unix::io::IntoRawFd;
                    let fd = f.into_raw_fd();
                    unsafe { redirect_fd_to(fd, 2) };
                }

                run_daemon();
                std::process::exit(0);
            }
        }
    }
}

// ── daemon core ───────────────────────────────────────────────────────────────

fn run_daemon() {
    log_info!(TAG, "start pid={}", std::process::id());

    let source = BatterySource::open().unwrap_or_else(|e| {
        log_error!(TAG, "battery source: {e}"); std::process::exit(1);
    });

    let mut reactor = Reactor::new().unwrap_or_else(|e| {
        log_error!(TAG, "reactor: {e}"); std::process::exit(1);
    });

    let listener = unix_socket::bind_unix_listener(
        UnixSocketAddr::Abstract(SOCKET),
        UnixSocketBindOptions::default(),
    )
    .unwrap_or_else(|e| {
        log_error!(TAG, "bind @{}: {e}", String::from_utf8_lossy(SOCKET));
        std::process::exit(1);
    });

    let source_tok   = reactor.add(source.fd(), true, false).expect("add source");
    let listener_tok = reactor.add(&listener.fd, true, false).expect("add listener");

    let mut watchers: HashMap<Token, UnixStreamFd> = HashMap::new();
    let mut events: Vec<Event> = Vec::new();
    let mut last_level: Option<u8> = read_level();

    // Initial charge state: inotify path reads sysfs directly; uevent path uses the property.
    let init_status = match &source {
        BatterySource::Inotify(_) => read_status(),
        BatterySource::Uevent(_)  => status_str(
            android_property_get("debug.tracing.battery_status")
                .as_deref()
                .unwrap_or(""),
        ).to_string(),
    };
    let _ = android_property_set(CHARGE_PROP, &init_status);

    log_info!(TAG, "source={} listening @{} level={:?} charge={init_status}",
              source.name(), String::from_utf8_lossy(SOCKET), last_level);

    loop {
        events.clear();
        match reactor.wait(&mut events, 16, -1) {
            Err(_) | Ok(0) => continue,
            Ok(_) => {}
        }

        let mut do_source  = false;
        let mut do_listener = false;
        let mut dead: Vec<Token> = Vec::new();

        for ev in &events {
            if ev.token == source_tok        { do_source   = true; }
            else if ev.token == listener_tok { do_listener = true; }
            else if ev.hangup || ev.error    { dead.push(ev.token); }
        }

        for tok in dead {
            if let Some(s) = watchers.remove(&tok) {
                log_info!(TAG, "watcher disconnected");
                let _ = reactor.del(&s.fd);
            }
        }

        if do_source {
            if let Some((cap, status)) = source.drain() {
                let _ = android_property_set(CHARGE_PROP, &status);
                if last_level != Some(cap) {
                    last_level = Some(cap);
                    log_info!(TAG, "level={cap} charge_state={status}");
                    let msg = format!("{cap}\n");
                    broadcast(&mut watchers, msg.as_bytes(), &mut reactor);
                }
            }
        }

        if do_listener {
            loop {
                match listener.accept_timeout(0) {
                    Ok(Some(stream)) => {
                        let level = last_level.unwrap_or_else(|| read_level().unwrap_or(0));
                        let msg   = format!("{level}\n");
                        if stream.fd.write_slice(msg.as_bytes()).is_ok() {
                            if let Ok(tok) = reactor.add(&stream.fd, true, false) {
                                log_info!(TAG, "watcher connected");
                                watchers.insert(tok, stream);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e)   => { log_warn!(TAG, "accept: {e}"); break; }
                }
            }
        }
    }
}

// ── entry point ───────────────────────────────────────────────────────────────

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
            let addr = UnixSocketAddr::Abstract(SOCKET);
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
