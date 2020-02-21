//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::{
    convert::TryInto,
    ffi::{c_void, CStr, CString},
    fmt,
    fs::OpenOptions,
    io,
    os::raw::c_int,
    os::unix::io::AsRawFd,
    path::Path,
    ptr,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use futures::channel::oneshot;
use nix::{convert_ioctl_res, errno::Errno, libc};
use snafu::{ResultExt, Snafu};

use spdk_sys::{
    spdk_iscsi_tgt_node_construct, spdk_nbd_disk,
    spdk_nbd_disk_find_by_nbd_path, spdk_nbd_get_path, spdk_nbd_start,
    spdk_nbd_stop,
};
use sysfs::parse_value;

use crate::{
    core::Reactors,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
    target,
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
/// Generate iqn based on provided uuid
fn target_name(uuid: &str) -> String {
    format!("iqn.2019-05.io.openebs:{}", uuid)
}
/// Start nbd disk using provided device name.
pub async fn start(
    bdev_name: &str,
    device_path: &str,
) -> Result<*mut spdk_nbd_disk, IscsiError> {
    let c_bdev_name_alt = CString::new("foo").unwrap();
    let c_bdev_name = CString::new(bdev_name).unwrap();
    let c_device_path = CString::new(device_path).unwrap();
    let (sender, receiver) =
        oneshot::channel::<ErrnoResult<*mut spdk_nbd_disk>>();

    info!(
        "(start) Started iSCSI disk {} for {}",
        device_path, bdev_name
    );

    let address = "127.0.0.1";
    if let Err(msg) = target::iscsi::init(&address, 1) {
        error!("Failed to initialize Mayastor iSCSI target: {}", msg);
        //return Err(EnvError::InitTarget {
        //    target: "iscsi".into(),
        //});
    }

    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let mut portal_group_idx: c_int = 1; // or 1
    let mut init_group_idx: c_int = 0; // or 1

    let mut lun_id: c_int = 0;
    /*let idx = ISCSI_IDX.with(move |iscsi_idx| {
        let idx = *iscsi_idx.borrow();
        *iscsi_idx.borrow_mut() = idx + 1;
        idx
    });*/
    let idx = 1; // does this also work with 0? yes but will probably fail if iscsi is used on the backend
    let tgt = unsafe {
        spdk_iscsi_tgt_node_construct(
            idx,                             // target_index
            c_iqn.as_ptr(),                  // name
            ptr::null(),                     // alias
            &mut portal_group_idx as *mut _, // pg_tag_list
            &mut init_group_idx as *mut _,   // ig_tag_list
            1, // portal and initiator group list length
            &mut c_bdev_name.as_ptr(),
            &mut lun_id as *mut _,
            1,     // length of lun id list
            128,   // max queue depth
            false, // disable chap
            false, // require chap
            false, // mutual chap
            0,     // chap group
            false, // header digest
            false, // data digest
        )
    };
    if tgt.is_null() {
        info!("Failed to create iscsi target {}", iqn);
    //Err(IscsiError::Unavailable {});
    } else {
        info!("Created iscsi target {}", iqn);
        //Ok(());
    }

    /*
    unsafe {
        spdk_nbd_start(
            c_bdev_name.as_ptr(),
            c_device_path.as_ptr(),
            Some(start_cb),
            cb_arg(sender),
        );
    }
    */
    info!(
        "(start) done creating iscsi target {} for {}",
        iqn, bdev_name
    );

    receiver
        .await
        .expect("Cancellation is not supported")
        .context(StartIscsi {
            dev: device_path.to_owned(),
        })
}

/// NBD disk representation.
pub struct IscsiTarget {
    iscsi_ptr: *mut spdk_nbd_disk,
}

impl IscsiTarget {
    /// Allocate nbd device for the bdev and start it.
    /// When the function returns the nbd disk is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, IscsiError> {
        // find a NBD device which is available
        let device_path = "";

        // call create_iscsi_disk here?

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

impl fmt::Debug for IscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_path(), self.iscsi_ptr)
    }
}

impl fmt::Display for IscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_path())
    }
}
