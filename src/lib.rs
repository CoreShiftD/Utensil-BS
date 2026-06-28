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

pub const TAG:            &str = "utensil-bs";
pub const SOCKET:         &[u8] = b"coreshift_batterystats";
pub const SOCKET_WATCHER: &[u8] = b"coreshift_bs_consumer";
pub const SYSFS_CAP:      &str  = "/sys/class/power_supply/battery/capacity";
pub const SYSFS_STATUS:   &str  = "/sys/class/power_supply/battery/status";
pub const CHARGE_PROP:    &str  = "debug.tracing.charge_state";
pub const DATA_DIR:       &str  = "/data/local/tmp/Utensil";
pub const PID_FILE:       &str  = "/data/local/tmp/Utensil/bs.pid";
pub const LOG_FILE:       &str  = "/data/local/tmp/Utensil/bs.log";

pub enum BatterySource {
    Inotify(Fd),
    Uevent(Fd),
}

impl BatterySource {
    pub fn open() -> Result<Self, coreshift_core::CoreError> {
        match inotify::init() {
            Ok(fd) => {
                let cap_ok = inotify::add_watch(&fd, SYSFS_CAP, inotify::MODIFY_MASK).is_ok();
                let _      = inotify::add_watch(&fd, SYSFS_STATUS, inotify::MODIFY_MASK);
                if cap_ok {
                    return Ok(BatterySource::Inotify(fd));
                }
            }
            Err(_) => {}
        }
        netlink::uevent_open().map(BatterySource::Uevent)
    }

    pub fn fd(&self) -> &Fd {
        match self {
            BatterySource::Inotify(fd) | BatterySource::Uevent(fd) => fd,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BatterySource::Inotify(_) => "inotify",
            BatterySource::Uevent(_)  => "uevent",
        }
    }

    pub fn drain(&self) -> Option<(u8, String)> {
        match self {
            BatterySource::Inotify(fd) => {
                let _ = inotify::read_events(fd);
                let cap    = read_level()?;
                let status = read_status();
                Some((cap, status))
            }
            BatterySource::Uevent(fd) => {
                let (msg_cap, _) = netlink::uevent_drain_battery(fd)?;
                let cap    = msg_cap.or_else(read_level)?;
                let status = read_status();
                Some((cap, status))
            }
        }
    }
}

pub fn read_level() -> Option<u8> {
    std::fs::read_to_string(SYSFS_CAP).ok()?.trim().parse().ok()
}

pub fn read_status() -> String {
    if let Ok(s) = std::fs::read_to_string(SYSFS_STATUS) {
        return s.trim().to_string();
    }
    // sysfs unavailable (permission denied) — fall back to prop
    status_str(
        android_property_get("debug.tracing.battery_status")
            .as_deref()
            .unwrap_or(""),
    ).to_string()
}

pub fn status_str(raw: &str) -> &'static str {
    match raw {
        "2" => "Charging",
        "3" => "Discharging",
        "4" => "Not charging",
        "5" => "Full",
        _   => "Unknown",
    }
}

pub fn broadcast(watchers: &mut HashMap<Token, UnixStreamFd>, msg: &[u8], reactor: &mut Reactor) {
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

pub fn connect() -> Result<UnixStreamFd, Box<dyn std::error::Error>> {
    match connect_unix_stream(UnixSocketAddr::Abstract(SOCKET))? {
        UnixConnectResult::Connected(s) => Ok(s),
        UnixConnectResult::InProgress(_) => Err("connection in progress".into()),
    }
}

pub fn connect_as_consumer() -> Result<UnixStreamFd, Box<dyn std::error::Error>> {
    match connect_unix_stream_named(
        UnixSocketAddr::Abstract(SOCKET),
        UnixSocketAddr::Abstract(SOCKET_WATCHER),
    )? {
        UnixConnectResult::Connected(s) => Ok(s),
        UnixConnectResult::InProgress(_) => Err("connection in progress".into()),
    }
}

pub fn cmd_watch() -> Result<(), Box<dyn std::error::Error>> {
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

pub fn cmd_uevent_dump() -> Result<(), Box<dyn std::error::Error>> {
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

pub fn cmd_status() -> Result<(), Box<dyn std::error::Error>> {
    let stream = connect().map_err(|_| "daemon not running")?;
    let mut buf = [0u8; 64];
    match stream.fd.read_slice(&mut buf)? {
        Some(n) if n > 0 => io::stdout().write_all(&buf[..n])?,
        _ => println!("no data"),
    }
    Ok(())
}

pub fn cmd_stop() -> Result<(), Box<dyn std::error::Error>> {
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

pub fn run_supervisor() -> Result<(), Box<dyn std::error::Error>> {
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

pub fn run_daemon() {
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

    let init_status = read_status();
    let _ = android_property_set(CHARGE_PROP, &init_status);

    log_info!(TAG, "source={} listening @{} level={:?} charge={init_status}",
              source.name(), String::from_utf8_lossy(SOCKET), last_level);

    loop {
        events.clear();
        match reactor.wait(&mut events, 16, -1) {
            Err(_) | Ok(0) => continue,
            Ok(_) => {}
        }

        let mut do_source   = false;
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
