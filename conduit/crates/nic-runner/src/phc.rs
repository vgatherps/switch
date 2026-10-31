//! Explicit Linux PHC selection. The file owns the dynamic clock ID's lifetime.

use std::fs::File;
use std::io;
use std::path::Path;

pub struct Phc(File);

impl Phc {
    pub fn open(path: &Path) -> io::Result<Self> {
        let phc = Self(File::open(path)?);
        phc.read()?;
        Ok(phc)
    }

    #[cfg(target_os = "linux")]
    pub fn read(&self) -> io::Result<u64> {
        use std::os::fd::AsRawFd;

        // Linux FD_TO_CLOCKID: CLOCKFD is 3. Keep the owning file alive across the syscall.
        let clock = ((!self.0.as_raw_fd() as libc::clockid_t) << 3) | 3;
        let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: time is writable, and self owns the fd encoded in clock for this call.
        if unsafe { libc::clock_gettime(clock, &mut time) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let ns = u64::try_from(time.tv_sec)
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .and_then(|ns| ns.checked_add(u64::try_from(time.tv_nsec).ok()?))
            .filter(|_| (0..1_000_000_000).contains(&time.tv_nsec));
        ns.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "PHC time is not representable in nanoseconds"))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn read(&self) -> io::Result<u64> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "--phc requires Linux dynamic clocks"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonexistent_device_is_an_open_error() {
        // A character device cannot contain a child, independent of the host's PHC devices.
        assert!(Phc::open(Path::new("/dev/null/ptp")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn open_device_that_is_not_a_phc_is_a_real_clock_read_error() {
        use std::os::fd::AsRawFd;

        assert_eq!(
            Phc::open(Path::new("/dev/null")).err().unwrap().raw_os_error(),
            Some(libc::EINVAL),
        );
        let phc = Phc(File::open("/dev/null").unwrap());
        let fd = phc.0.as_raw_fd();
        // SAFETY: F_GETFD only inspects this still-owned descriptor.
        assert!(unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0);
        let error = phc.read().unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        // A failed clock read must not close or replace the selected device.
        assert_eq!(phc.0.as_raw_fd(), fd);
        assert!(phc.read().is_err());
    }
}
