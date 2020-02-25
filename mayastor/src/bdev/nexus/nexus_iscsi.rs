//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::{
    fmt,
};

use nix::{errno::Errno};
use snafu::{Snafu};

use crate::{
    target::iscsi::construct_iscsi_target,
    target::iscsi::ISCSI_PORTAL_GROUP_FE,
    target::iscsi::ISCSI_INITIATOR_GROUP,
    target::iscsi::target_name,
    target::iscsi::unshare_generic,
    
    //target::iscsi::Error,
};

#[derive(Debug, Snafu)]
pub enum IscsiError {
    #[snafu(display("No free NBD devices available (is nbd kmod loaded?)"))]
    Unavailable {},
    #[snafu(display("Failed to start iscsi target on {}", dev))]
    StartIscsi { source: Errno, dev: String },
}

/// Start nbd disk using provided device name.
pub async fn start(
    bdev_name: &str,
) -> Result<String, IscsiError> {

    info!("(start) Started iSCSI disk for {}", bdev_name);

    match construct_iscsi_target(bdev_name,
        ISCSI_PORTAL_GROUP_FE,
        ISCSI_INITIATOR_GROUP) {
        Ok(_) => {
            info!("(start) done creating iscsi target for {}", bdev_name);
            let target_name = target_name(bdev_name);
            return Ok(target_name)
        },
        Err(_) => return Err(IscsiError::Unavailable{ }),
    }
}

/// Iscsi target representation.
pub struct IscsiTarget {
    iscsi_ptr: String, // fixme
}

impl IscsiTarget {
    /// Allocate iscsi device for the bdev and start it.
    /// When the function returns the iscsi target is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, IscsiError> {

        let iscsi_ptr = start(bdev_name).await?;

        info!("Started iscsi target for {}", bdev_name);

        Ok(Self { iscsi_ptr })
    }

    /// Stop and release nbd device.
    pub async fn destroy(self) {
        info!("Destroying iscsi frontend target");
        match unshare_generic(&self.iscsi_ptr, ISCSI_PORTAL_GROUP_FE).await {
            Ok(_) => (),
            Err(_) =>  error!("Failed to destroy iscsi frontend target"),
        }
    }

    /// Get nbd device path (/dev/nbd...) for the nbd disk.
    pub fn get_path(&self) -> String {
        //unsafe {
        //    CStr::from_ptr(spdk_nbd_get_path(self.iscsi_ptr))
        //        .to_str()
        //        .unwrap()
        //        .to_string()
        //}
        return "".to_string(); // fixme
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
