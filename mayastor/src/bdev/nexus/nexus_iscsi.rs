//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::{
    fmt,
};

use snafu::{Snafu};

use crate::{
    core::Bdev,
    target::iscsi::construct_iscsi_target,
    target::iscsi::ISCSI_PORTAL_GROUP_FE,
    target::iscsi::ISCSI_INITIATOR_GROUP,
    target::iscsi::target_name,
    target::iscsi::unshare,
};

#[derive(Debug, Snafu)]
pub enum NexusIscsiError {
    #[snafu(display("Failed to create iscsi target for bdev uuid {}, error {}", dev, err))]
    CreateTargetFailed { dev: String, err: String },
    #[snafu(display("Bdev not found {}", dev))]
    BdevNotFound { dev: String },
}

/// Iscsi target representation.
pub struct NexusIscsiTarget {
    bdev_name: String,  // logically we might store a spdk_iscsi_tgt_node here but ATM the bdev name is all we actually need
}

impl NexusIscsiTarget {
    /// Allocate iscsi device for the bdev and start it.
    /// When the function returns the iscsi target is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, NexusIscsiError> {

        let bdev = match Bdev::lookup_by_name(bdev_name) {
            None => return Err(NexusIscsiError::BdevNotFound{ dev: bdev_name.to_string() }),
            Some(bd) => bd,
        };

        match construct_iscsi_target(bdev_name,
            &bdev,
            ISCSI_PORTAL_GROUP_FE,
            ISCSI_INITIATOR_GROUP) {
            Ok(_) => Ok(Self { bdev_name: bdev_name.to_string() }),
            Err(e) => Err(NexusIscsiError::CreateTargetFailed{ dev: bdev_name.to_string(), err: e.to_string() }),
        }
    }

    pub async fn destroy(self) {
        info!("Destroying iscsi frontend target");
        match unshare(&self.bdev_name).await {
            Ok(_) => (),
            Err(e) =>  error!("Failed to destroy iscsi frontend target {}", e),
        }
    }

    pub fn get_iqn(&self) -> String {
        return target_name(&self.bdev_name);
    }
}

impl fmt::Debug for NexusIscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_iqn(), self.bdev_name)
    }
}

impl fmt::Display for NexusIscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_iqn())
    }
}
