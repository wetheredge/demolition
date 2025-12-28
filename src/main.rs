use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::SystemTime;
use std::{fs, io};

use rustix::mount::{MountFlags, UnmountFlags, mount, unmount};

const EXIT_ENV: i32 = 1;
const EXIT_ERR: i32 = 2;

#[macro_export]
macro_rules! env {
    (os, $e:literal, $dbg:literal) => {{
        #[cfg(debug_assertions)]
        let v = ::std::env::var_os($e).unwrap_or($dbg.into());
        #[cfg(not(debug_assertions))]
        let Some(v) = ::std::env::var_os($e) else {
            $crate::env!(bail, "env var is not set: ", $e);
        };
        v
    }};
    (str, $e:literal, $dbg:literal) => {
        match $crate::env!(os, $e, $dbg).into_string() {
            Ok(v) => v,
            Err(_) => $crate::env!(bail, "env var is not unicode: ", $e),
        }
    };
    (from_str($t:ty), $e:literal, $dbg:literal) => {
        match $crate::env!(str, $e, $dbg).parse::<$t>() {
            Ok(v) => v,
            Err(err) => $crate::env!(bail, "invalid format for ", $e, ": {}"; err),
        }
    };
    (parse($parse:path), $e:literal, $dbg:literal) => {
        match $parse($crate::env!(str, $e, $dbg).as_str()) {
            Ok(v) => v,
            Err(err) => $crate::env!(bail, "invalid format for ", $e, ": {}"; err),
        }
    };
    (bail, $($err:literal),+ $(; $($rest:tt)*)?) => {{
        ::log::error!(::std::concat!($($err),+) $(, $($rest)*)?);
        ::std::process::exit($crate::EXIT_ENV);
    }};
}

#[macro_export]
macro_rules! bail {
    ($($t:tt)+) => {{
        ::log::error!($($t)+);
        ::std::process::exit($crate::EXIT_ERR);
    }};
}

#[macro_export]
macro_rules! unwrap {
    ($e:expr, $($err:tt),+) => {
        match $e {
            Ok(ok) => ok,
            Err(err) => $crate::bail!($($err,)+ err = err),
        }
    };
}

macro_rules! log_dry {
    ($msg:literal $($rest:tt)*) => {
        ::log::info!(::std::concat!("dry run: ", $msg) $($rest)*)
    };
}

fn main() {
    env_logger::Builder::from_env("DEMOLITION_LOG").init();

    let mount_dir = &PathBuf::from(env!(os, "DEMOLITION_MOUNT_DIR", "./mnt"));
    let root_volume = mount_dir.join(env!(os, "DEMOLITION_ROOT_VOLUME", "root"));
    let backups_dir = mount_dir.join(env!(os, "DEMOLITION_BACKUP_DIR", "root-backups"));
    let backup_format = env!(str, "DEMOLITION_BACKUP_FORMAT", "%Y%m%d_%H%M%S");
    let keep_during = env!(
        parse(humantime::parse_duration),
        "DEMOLITION_KEEP_DURATION",
        "1day"
    );
    let keep_count = env!(from_str(u16), "DEMOLITION_KEEP_COUNT", "1");
    let dry_run = cfg!(debug_assertions);

    match fs::create_dir(mount_dir) {
        Ok(()) => log::debug!("created mount point"),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            log::warn!("mount point already exists");
        }
        Err(err) => bail!("failed to create mount point: {err}"),
    }

    let flags = MountFlags::NOATIME | MountFlags::NODEV | MountFlags::NOEXEC | MountFlags::NOSUID;
    if let Err(err) = mount("/dev/mapper/crypted", mount_dir, "btrfs", flags, None) {
        bail!("mount failed: {err}");
    };

    match root_volume.metadata().and_then(|m| m.created()) {
        Ok(created) => {
            let created = chrono::DateTime::from(created);
            let created = created.format(&backup_format).to_string();
            let backup = backups_dir.join(created);

            if dry_run {
                log_dry!("mv '{}' '{}'", root_volume.display(), backup.display());
            } else if let Err(err) = fs::rename(root_volume, backup) {
                bail!("failed to move existing root volume into backups: {err}");
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            log::debug!("no old root volume found");
        }
        Err(err) => bail!("failed to get root volume creation date: {err}"),
    }

    let mut backups = Vec::new();
    let entries = unwrap!(
        backups_dir.read_dir(),
        "failed to get entries of backups directory: {err}"
    );
    for entry in entries {
        let entry = match entry {
            Ok(ok) => ok,
            Err(err) => {
                log::warn!("skipping backup: {err}");
                continue;
            }
        };

        let modified = unwrap!(
            entry.metadata().and_then(|m| m.modified()),
            "failed to get backup creation date: {err}"
        );
        let age = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        backups.push((age, entry));
    }
    backups.sort_unstable_by_key(|(age, _)| *age);

    let remove_count = backups.len().saturating_sub(keep_count.into());
    log::trace!("removing up to {remove_count} of {} backups", backups.len());
    let remove = backups
        .into_iter()
        .take(remove_count)
        .take_while(|(age, _)| *age > keep_during);
    for (_, backup) in remove {
        log::trace!("removing backup: {}", backup.path().display());

        if dry_run {
            log_dry!(
                "btrfs subvolume delete --recursive '{}'",
                backup.path().display()
            );
            continue;
        }

        let mut cmd = Command::new("btrfs");
        cmd.args(["subvolume", "delete", "--recursive"]);
        cmd.arg(backup.path());
        cmd.stdin(Stdio::null());
        match cmd.status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                if let Some(code) = status.code() {
                    log::warn!(
                        "btrfs subvolume delete '{}' exitted with {code}",
                        backup.path().display()
                    )
                } else {
                    log::warn!(
                        "btrfs subvolume delete '{}' exitted with unknown exit code",
                        backup.path().display()
                    )
                }
            }
            Err(err) => {
                log::warn!(
                    "failed to get btrfs exit code while removing backup '{}': {err}",
                    backup.path().display()
                );
            }
        }
    }

    if let Err(err) = unmount(mount_dir, UnmountFlags::empty()) {
        bail!("umount failed: {err}");
    }
}
