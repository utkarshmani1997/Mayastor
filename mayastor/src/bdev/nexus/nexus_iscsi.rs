//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::{
    fmt,
};

use snafu::{Snafu};

use crate::{
    target::iscsi::construct_iscsi_target,
    target::iscsi::ISCSI_PORTAL_GROUP_FE,
    target::iscsi::ISCSI_INITIATOR_GROUP,
    target::iscsi::unshare,
};

#[derive(Debug, Snafu)]
pub enum IscsiError {
    #[snafu(display("Failed to start iscsi target for bdev uuid {}", dev))]
    StartIscsi { dev: String },
}

/// Start iscsi target using given bdev name.
pub async fn start(
    bdev_name: &str,
) -> Result<String, IscsiError> {

    info!("(start) Started iSCSI disk for {}", bdev_name);

    match construct_iscsi_target(bdev_name,
        ISCSI_PORTAL_GROUP_FE,
        ISCSI_INITIATOR_GROUP) {
        Ok(_) => {
            info!("(start) done creating iscsi target for {}", bdev_name);
            return Ok(bdev_name.to_string())
        },
        Err(_) => return Err(IscsiError::StartIscsi{ dev: bdev_name.to_string() }),
    }
}

/// Iscsi target representation.
pub struct IscsiTarget {
    bdev_uuid_str: String, // this is the bdev name (uuid)
}

impl IscsiTarget {
    /// Allocate iscsi device for the bdev and start it.
    /// When the function returns the iscsi target is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, IscsiError> {

        let bdev_name = start(bdev_name).await?;

        info!("Started iscsi target for {}", bdev_name);

        Ok(Self { bdev_uuid_str: bdev_name.to_string() })
    }

    /// Stop and release iscsi device.
    pub async fn destroy(self) {
        info!("Destroying iscsi frontend target");
        match unshare(&self.bdev_uuid_str).await {
            Ok(_) => (),
            Err(_) =>  error!("Failed to destroy iscsi frontend target"),
        }
    }

    /// Get device path actually means bdev_uuid in this case
    pub fn get_path(&self) -> String {
        return self.bdev_uuid_str.clone();
    }
}

impl fmt::Debug for IscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_path(), self.bdev_uuid_str)
    }
}

impl fmt::Display for IscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_path())
    }
}
