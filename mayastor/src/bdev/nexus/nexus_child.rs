use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use spdk_sys::{spdk_bdev_module_release_bdev, spdk_io_channel};

use crate::{
    bdev::nexus::nexus_label::{
        GPTHeader,
        GptEntry,
        LabelData,
        NexusLabel,
        Pmbr,
    },
    core::{Bdev, BdevHandle, CoreError, Descriptor, DmaBuf, DmaError},
    nexus_uri::{bdev_destroy, BdevCreateDestroy},
};

#[derive(Debug, Snafu)]
pub enum ChildError {
    #[snafu(display("Child is not closed"))]
    ChildNotClosed {},
    #[snafu(display(
        "Child is smaller than parent {} vs {}",
        child_size,
        parent_size
    ))]
    ChildTooSmall { child_size: u64, parent_size: u64 },
    #[snafu(display("Open child"))]
    OpenChild { source: CoreError },
    #[snafu(display("Claim child"))]
    ClaimChild { source: Errno },
    #[snafu(display("Child is read-only"))]
    ChildReadOnly {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Failed to allocate buffer for label"))]
    LabelAlloc { source: DmaError },
    #[snafu(display("Failed to read label from child"))]
    LabelRead { source: ChildIoError },
    #[snafu(display("Label is invalid"))]
    LabelInvalid {},
    #[snafu(display("Failed to allocate buffer for partition table"))]
    PartitionTableAlloc { source: DmaError },
    #[snafu(display("Failed to read partition table from child"))]
    PartitionTableRead { source: ChildIoError },
    #[snafu(display("Invalid partition table"))]
    InvalidPartitionTable {},
    #[snafu(display("Invalid partition table checksum"))]
    PartitionTableChecksum {},
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
    #[snafu(display("Failed to create a BdevHandle for child"))]
    HandleCreate { source: CoreError },
}

#[derive(Debug, Snafu)]
pub enum ChildIoError {
    #[snafu(display("Error writing to {}", name))]
    WriteError { source: CoreError, name: String },
    #[snafu(display("Error reading from {}", name))]
    ReadError { source: CoreError, name: String },
    #[snafu(display("Invalid descriptor for child bdev {}", name))]
    InvalidDescriptor { name: String },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ChildState {
    /// child has not been opened, but we are in the process of opening it
    Init,
    /// cannot add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// The child has been closed by its parent
    Closed,
    /// a non-fatal have occurred on this child
    Faulted,
}

impl ToString for ChildState {
    fn to_string(&self) -> String {
        match *self {
            ChildState::Init => "init",
            ChildState::ConfigInvalid => "configInvalid",
            ChildState::Open => "open",
            ChildState::Faulted => "faulted",
            ChildState::Closed => "closed",
        }
        .parse()
        .unwrap()
    }
}

#[derive(Debug, Serialize)]
pub struct NexusChild {
    /// name of the parent this child belongs too
    pub(crate) parent: String,
    /// Name of the child is the URI used to create it.
    /// Note that bdev name can differ from it!
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    /// channel on which we submit the IO
    pub(crate) ch: *mut spdk_io_channel,
    #[serde(skip_serializing)]
    pub(crate) desc: Option<Arc<Descriptor>>,
    /// current state of the child
    pub(crate) state: ChildState,
    pub(crate) repairing: bool,
    /// descriptor obtained after opening a device
    #[serde(skip_serializing)]
    pub(crate) bdev_handle: Option<BdevHandle>,
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        if self.bdev.is_some() {
            let bdev = self.bdev.as_ref().unwrap();
            writeln!(
                f,
                "{}: {:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state,
                bdev.num_blocks(),
                bdev.block_len(),
            )
        } else {
            writeln!(f, "{}: state {:?}", self.name, self.state)
        }
    }
}

impl NexusChild {
    /// Open the child in RW mode and claim the device to be ours. If the child
    /// is already opened by someone else (i.e one of the targets) it will
    /// error out.
    ///
    /// only devices in the closed or Init state can be opened.
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        if self.state != ChildState::Closed && self.state != ChildState::Init {
            return Err(ChildError::ChildNotClosed {});
        }

        if self.bdev.is_none() {
            return Err(ChildError::OpenWithoutBdev {});
        }

        let bdev = self.bdev.as_ref().unwrap();

        let child_size = bdev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{}: child to small parent size: {} child size: {}",
                self.name, parent_size, child_size
            );
            self.state = ChildState::ConfigInvalid;
            return Err(ChildError::ChildTooSmall {
                parent_size,
                child_size,
            });
        }

        self.desc = Some(Arc::new(
            Bdev::open_by_name(&bdev.name(), true).context(OpenChild {})?,
        ));

        self.bdev_handle = Some(
            BdevHandle::try_from(self.desc.as_ref().unwrap().clone()).unwrap(),
        );

        self.state = ChildState::Open;

        debug!("{}: child {} opened successfully", self.parent, self.name);

        Ok(self.name.clone())
    }

    /// return a descriptor to this child
    pub fn get_descriptor(&self) -> Result<Arc<Descriptor>, CoreError> {
        if let Some(ref d) = self.desc {
            Ok(d.clone())
        } else {
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// close the bdev -- we have no means of determining if this succeeds
    pub(crate) fn close(&mut self) -> ChildState {
        trace!("{}: Closing child {}", self.parent, self.name);

        if let Some(bdev) = self.bdev.as_ref() {
            unsafe {
                if !(*bdev.as_ptr()).internal.claim_module.is_null() {
                    spdk_bdev_module_release_bdev(bdev.as_ptr());
                }
            }
        }

        // just to be explicit
        let hdl = self.bdev_handle.take();
        let desc = self.desc.take();
        drop(hdl);
        drop(desc);

        // we leave the child structure around for when we want reopen it
        self.state = ChildState::Closed;
        self.state
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: None,
            ch: std::ptr::null_mut(),
            state: ChildState::Init,
            bdev_handle: None,
            repairing: false,
        }
    }

    /// destroy the child bdev
    pub(crate) async fn destroy(&mut self) -> Result<(), BdevCreateDestroy> {
        assert_eq!(self.state, ChildState::Closed);
        if let Some(_bdev) = &self.bdev {
            bdev_destroy(&self.name).await
        } else {
            warn!("Destroy child without bdev");
            Ok(())
        }
    }

    /// returns if a child can be written to
    pub fn can_rw(&self) -> bool {
        self.state == ChildState::Open || self.state == ChildState::Faulted
    }

    /// read and validate this child's label
    pub async fn probe_label(&mut self) -> Result<NexusLabel, ChildError> {
        if !self.can_rw() {
            info!(
                "{}: Trying to read from closed child: {}",
                self.parent, self.name
            );
            return Err(ChildError::ChildReadOnly {});
        }

        let bdev = match self.bdev.as_ref() {
            Some(dev) => dev,
            None => {
                return Err(ChildError::ChildInvalid {});
            }
        };

        let desc = match self.bdev_handle.as_ref() {
            Some(handle) => handle,
            None => {
                return Err(ChildError::ChildInvalid {});
            }
        };

        let block_size = bdev.block_len();
        let mut buf = desc
            .dma_malloc(block_size as usize)
            .context(LabelAlloc {})?;

        self.read_at(0, &mut buf).await.context(LabelRead {})?;
        let mbr = match Pmbr::from_slice(&buf.as_slice()[440 .. 512]) {
            Ok(record) => record,
            Err(_) => {
                warn!(
                    "{}: {}: The protective MBR is invalid!",
                    self.parent, self.name
                );
                return Err(ChildError::LabelInvalid {});
            }
        };

        self.read_at(u64::from(block_size), &mut buf)
            .await
            .context(LabelRead {})?;
        let primary = match GPTHeader::from_slice(buf.as_slice()) {
            Ok(header) => header,
            Err(_) => {
                warn!(
                    "{}: {}: The primary GPT header is invalid!",
                    self.parent, self.name
                );
                return Err(ChildError::LabelInvalid {});
            }
        };

        if mbr.entries[0].num_sectors != 0xffff_ffff
            && mbr.entries[0].num_sectors as u64 != primary.lba_alt
        {
            warn!("{}: {}: The protective MBR disk size does not match the GPT disk size!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        self.read_at((bdev.num_blocks() - 1) * u64::from(block_size), &mut buf)
            .await
            .context(LabelRead {})?;
        let secondary = match GPTHeader::from_slice(buf.as_slice()) {
            Ok(header) => header,
            Err(_) => {
                warn!(
                    "{}: {}: The secondary GPT header is invalid!",
                    self.parent, self.name
                );
                return Err(ChildError::LabelInvalid {});
            }
        };

        if primary.guid != secondary.guid {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: GUIDs differ!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        if primary.lba_start != secondary.lba_start
            || primary.lba_end != secondary.lba_end
        {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: disk sizes differ!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        if primary.table_crc != secondary.table_crc {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: stored partition table checksums differ!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        // determine number of blocks we need to read from the partition table
        let num_blocks =
            ((primary.entry_size * primary.num_entries) / block_size) + 1;
        let mut buf = desc
            .dma_malloc((num_blocks * block_size) as usize)
            .context(PartitionTableAlloc {})?;
        self.read_at(primary.lba_table * u64::from(block_size), &mut buf)
            .await
            .context(PartitionTableRead {})?;
        let mut partitions =
            match GptEntry::from_slice(&buf.as_slice(), primary.num_entries) {
                Ok(table) => table,
                Err(_) => {
                    warn!(
                        "{}: {}: The partition table is invalid!",
                        self.parent, self.name
                    );
                    return Err(ChildError::InvalidPartitionTable {});
                }
            };

        if GptEntry::checksum(&partitions) != primary.table_crc {
            warn!("{}: {}: The calculated and stored partition table checksums differ!", self.parent, self.name);
            return Err(ChildError::PartitionTableChecksum {});
        }

        // some tools write 128 partition entries, even though only two are
        // created, in any case we are only ever interested in the first two
        // partitions, so we drain the others.
        let parts = partitions.drain(.. 2).collect::<Vec<_>>();

        Ok(NexusLabel {
            mbr,
            primary,
            partitions: parts,
            secondary,
        })
    }

    /// write a label to this child
    pub async fn write_label(
        &mut self,
        label: &NexusLabel,
        data: &LabelData,
        block_size: u64,
    ) -> Result<(), ChildIoError> {
        // Protective MBR
        self.write_at(0 as u64, &data.mbr).await?;

        // Primary GPT header
        self.write_at(block_size * label.primary.lba_self, &data.primary)
            .await?;

        // Primary partition table
        self.write_at(block_size * label.primary.lba_table, &data.table)
            .await?;

        // Secondary partition table
        self.write_at(block_size * label.secondary.lba_table, &data.table)
            .await?;

        // Secondary GPT header
        self.write_at(block_size * label.secondary.lba_self, &data.secondary)
            .await?;

        Ok(())
    }

    /// write the contents of the buffer to this child
    pub async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, ChildIoError> {
        if let Some(desc) = self.bdev_handle.as_ref() {
            Ok(desc.write_at(offset, buf).await.context(WriteError {
                name: self.name.clone(),
            })?)
        } else {
            Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// read from this child device into the given buffer
    pub async fn read_at(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<usize, ChildIoError> {
        if let Some(desc) = self.bdev_handle.as_ref() {
            Ok(desc.read_at(offset, buf).await.context(ReadError {
                name: self.name.clone(),
            })?)
        } else {
            Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }
}
