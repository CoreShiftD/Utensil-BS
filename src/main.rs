// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/

use coreshift_core::android_property::{android_property_get, android_property_set};
use coreshift_core::netlink;
use coreshift_core::reactor::{Event, Reactor, Token};
use coreshift_core::unix_socket::{self, UnixSocketAddr, UnixSocketBindOptions, UnixStreamFd};
use coreshift_core::{log_error, log_info, log_warn};
use std::collections::HashMap;

const TAG:         &str = "utensil-bs";
const SOCKET:      &[u8] = b"coreshift_batterystats";
const SYSFS_CAP:   &str = "/sys/class/power_supply/battery/capacity";
const CHARGE_PROP: &str = "debug.tracing.charge_state";

fn read_level() -> Option<u8> {
    std::fs::read_to_string(SYSFS_CAP).ok()?.trim().parse().ok()
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

fn main() {
    log_info!(TAG, "start pid={}", std::process::id());

    let mut reactor = Reactor::new().unwrap_or_else(|e| {
        log_error!(TAG, "reactor: {e}"); std::process::exit(1);
    });

    let uevent_fd = netlink::uevent_open().unwrap_or_else(|e| {
        log_error!(TAG, "netlink uevent: {e}"); std::process::exit(1);
    });

    let listener = unix_socket::bind_unix_listener(
        UnixSocketAddr::Abstract(SOCKET),
        UnixSocketBindOptions::default(),
    )
    .unwrap_or_else(|e| {
        log_error!(TAG, "bind @{}: {e}", String::from_utf8_lossy(SOCKET));
        std::process::exit(1);
    });

    let uevent_tok   = reactor.add(&uevent_fd,   true, false).expect("add uevent");
    let listener_tok = reactor.add(&listener.fd, true, false).expect("add listener");

    let mut watchers: HashMap<Token, UnixStreamFd> = HashMap::new();
    let mut events: Vec<Event> = Vec::new();
    let mut last_level: Option<u8> = read_level();

    // Publish initial charge state from existing property.
    let init_status = status_str(
        android_property_get("debug.tracing.battery_status")
            .as_deref()
            .unwrap_or(""),
    );
    let _ = android_property_set(CHARGE_PROP, init_status);

    log_info!(TAG, "listening @{} level={:?} charge={init_status}",
              String::from_utf8_lossy(SOCKET), last_level);

    loop {
        events.clear();
        match reactor.wait(&mut events, 16, -1) {
            Err(_) | Ok(0) => continue,
            Ok(_) => {}
        }

        let mut do_uevent   = false;
        let mut do_listener = false;
        let mut dead: Vec<Token> = Vec::new();

        for ev in &events {
            if ev.token == uevent_tok        { do_uevent   = true; }
            else if ev.token == listener_tok { do_listener = true; }
            else if ev.hangup || ev.error    { dead.push(ev.token); }
        }

        for tok in dead {
            if let Some(s) = watchers.remove(&tok) {
                log_info!(TAG, "watcher disconnected");
                let _ = reactor.del(&s.fd);
            }
        }

        if do_uevent {
            if let Some((cap, status)) = netlink::uevent_drain_battery(&uevent_fd) {
                // Always update the charge state property (fires even if level unchanged).
                let _ = android_property_set(CHARGE_PROP, &status);

                // Stream level to watchers only on change.
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
                        // Send current level immediately on connect.
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
