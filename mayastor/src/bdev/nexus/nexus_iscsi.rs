//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::{
    convert::TryInto,
    ffi::{c_void, CStr, CString},
    fmt,
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use futures::channel::oneshot;
use nix::{convert_ioctl_res, errno::Errno, libc};
use snafu::{ResultExt, Snafu};

use spdk_sys::{
    spdk_nbd_disk, spdk_nbd_disk_find_by_nbd_path, spdk_nbd_get_path,
    spdk_nbd_start, spdk_nbd_stop,
};
use sysfs::parse_value;

use crate::{
    core::Reactors,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
};

// include/uapi/linux/fs.h
const IOCTL_BLKGETSIZE: u32 = ior!(0x12, 114, std::mem::size_of::<u64>());

#[derive(Debug, Snafu)]
pub enum IscsiError {
    #[snafu(display("No free NBD devices available (is nbd kmod loaded?)"))]
    Unavailable {},
    #[snafu(display("Failed to start NBD on {}", dev))]
    StartIscsi { source: Errno, dev: String },
}

/// Callback for spdk_nbd_start().
extern "C" fn start_cb(
    sender_ptr: *mut c_void,
    iscsi_disk: *mut spdk_nbd_disk,
    errno: i32,
) {
    let sender = unsafe {
        Box::from_raw(
            sender_ptr as *mut oneshot::Sender<ErrnoResult<*mut spdk_nbd_disk>>,
        )
    };
    sender
        .send(errno_result_from_i32(iscsi_disk, errno))
        .expect("NBD start receiver is gone");
}

/// Start nbd disk using provided device name.
pub async fn start(
    bdev_name: &str,
    device_path: &str,
) -> Result<*mut spdk_nbd_disk, IscsiError> {
    let c_bdev_name = CString::new(bdev_name).unwrap();
    let c_device_path = CString::new(device_path).unwrap();
    let (sender, receiver) =
        oneshot::channel::<ErrnoResult<*mut spdk_nbd_disk>>();

    info!(
        "(start) Started iSCSI disk {} for {}",
        device_path, bdev_name
    );

    unsafe {
        spdk_nbd_start(
            c_bdev_name.as_ptr(),
            c_device_path.as_ptr(),
            Some(start_cb),
            cb_arg(sender),
        );
    }
    info!(
        "(start) done Started nbd disk {} for {}",
        device_path, bdev_name
    );

    receiver
        .await
        .expect("Cancellation is not supported")
        .context(StartIscsi {
            dev: device_path.to_owned(),
        })
}

/// NBD disk representation.
pub struct IscsiDisk {
    iscsi_ptr: *mut spdk_nbd_disk,
}

impl IscsiDisk {
    /// Allocate nbd device for the bdev and start it.
    /// When the function returns the nbd disk is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, IscsiError> {
        // find a NBD device which is available
        let device_path = "";
        let iscsi_ptr = start(bdev_name, &device_path).await?;

        // we wait for the dev to come up online because
        // otherwise the mount done too early would fail.
        // If it times out, continue anyway and let the mount fail.
        //wait_until_ready(&device_path).unwrap();
        info!("Started iscsi disk {} for {}", device_path, bdev_name);

        Ok(Self { iscsi_ptr })
    }

    /// Stop and release nbd device.
    pub fn destroy(self) {
        unsafe { spdk_nbd_stop(self.iscsi_ptr) };
    }

    /// Get nbd device path (/dev/nbd...) for the nbd disk.
    pub fn get_path(&self) -> String {
        unsafe {
            CStr::from_ptr(spdk_nbd_get_path(self.iscsi_ptr))
                .to_str()
                .unwrap()
                .to_string()
        }
    }
}

impl fmt::Debug for IscsiDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_path(), self.iscsi_ptr)
    }
}

impl fmt::Display for IscsiDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_path())
    }
}
