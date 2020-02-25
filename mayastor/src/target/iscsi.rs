//! Methods for creating iscsi targets.
//!
//! We create a wildcard portal and initiator groups when mayastor starts up.
//! These groups allow unauthenticated access for any initiator. Then when
//! exporting a replica we use these default groups and create one target per
//! replica with one lun - LUN0.

use std::{
    cell::RefCell,
    ffi::CString,
    os::raw::{c_char, c_int},
    ptr,
};

use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};

use spdk_sys::{
    spdk_iscsi_find_tgt_node,
    spdk_iscsi_init_grp_create_from_initiator_list,
    spdk_iscsi_init_grp_destroy,
    spdk_iscsi_init_grp_unregister,
    spdk_iscsi_portal_create,
    spdk_iscsi_portal_grp_add_portal,
    spdk_iscsi_portal_grp_create,
    spdk_iscsi_portal_grp_open,
    spdk_iscsi_portal_grp_register,
    spdk_iscsi_portal_grp_release,
    spdk_iscsi_portal_grp_unregister,
    spdk_iscsi_shutdown_tgt_node_by_name,
    spdk_iscsi_tgt_node,
    spdk_iscsi_tgt_node_construct,
};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
    jsonrpc::{Code, RpcErrorCode},
};

/// iSCSI target related errors
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create default portal group"))]
    CreatePortalGroup {},
    #[snafu(display("Failed to create default iscsi portal"))]
    CreatePortal {},
    #[snafu(display("Failed to add default portal to portal group"))]
    AddPortal {},
    #[snafu(display("Failed to register default portal group"))]
    RegisterPortalGroup {},
    #[snafu(display("Failed to create default initiator group"))]
    CreateInitiatorGroup {},
    #[snafu(display("Failed to create iscsi target"))]
    CreateTarget {},
    #[snafu(display("Failed to destroy iscsi target"))]
    DestroyTarget { source: Errno },
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        Code::InternalError
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// iscsi target port number
pub const ISCSI_PORT_FE: u16 = 3260;
pub const ISCSI_PORT_BE: u16 = 3262;

pub const ISCSI_PORTAL_GROUP_FE: c_int = 2;
pub const ISCSI_PORTAL_GROUP_BE: c_int = 0;

pub const ISCSI_INITIATOR_GROUP: c_int = 0; //only 1 for now

thread_local! {
    /// iscsi global state.
    ///
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    ///
    /// A counter used for assigning idx to newly created iscsi targets.
    static ISCSI_IDX: RefCell<i32> = RefCell::new(0);
    /// IP address of iscsi portal used for all created iscsi targets.
    static ADDRESS: RefCell<Option<String>> = RefCell::new(None);
}

/// Generate iqn based on provided uuid
pub fn target_name(uuid: &str) -> String {
    format!("iqn.2019-05.io.openebs:{}", uuid)
}

/// Create iscsi portal and initiator group which will be used later when
/// creating iscsi targets.
pub fn init(address: &str) -> Result<()> {
    let initiator_host = CString::new("ANY").unwrap();
    let initiator_netmask = CString::new("ANY").unwrap();

    info!("Creating portal group for address {}", address);

    init_portal_group(address, ISCSI_PORT_BE, ISCSI_PORTAL_GROUP_BE)?;
    init_portal_group(address, ISCSI_PORT_FE, ISCSI_PORTAL_GROUP_FE)?;

    unsafe {
        if spdk_iscsi_init_grp_create_from_initiator_list(
            ISCSI_INITIATOR_GROUP,
            1,
            &mut (initiator_host.as_ptr() as *mut c_char) as *mut _,
            1,
            &mut (initiator_netmask.as_ptr() as *mut c_char) as *mut _,
        ) != 0
        {
            fini();
            return Err(Error::CreateInitiatorGroup {});
        }
    }
    ADDRESS.with(move |addr| {
        *addr.borrow_mut() = Some(address.to_owned());
    });
    debug!("Created default iscsi initiator group");

    Ok(())
}

/// Destroy iscsi default portal and initiator group.
pub fn fini() {
    unsafe {
        let ig = spdk_iscsi_init_grp_unregister(0);
        if !ig.is_null() {
            spdk_iscsi_init_grp_destroy(ig);
        }
        let pg0 = spdk_iscsi_portal_grp_unregister(0);
        if !pg0.is_null() {
            spdk_iscsi_portal_grp_release(pg0);
        }
        let pg1 = spdk_iscsi_portal_grp_unregister(1);
        if !pg1.is_null() {
            spdk_iscsi_portal_grp_release(pg1);
        }
    }
}

/// Export given bdev over iscsi. That involves creating iscsi target and
/// adding the bdev as LUN to it.
pub fn share(uuid: &str, _bdev: &Bdev) -> Result<()> {

    let tgt = construct_iscsi_target(uuid, ISCSI_PORTAL_GROUP_BE, ISCSI_INITIATOR_GROUP);

    match tgt {
        Ok(_tgt) => {
            info!(
                "(start) done creating iscsi backend target for {}",
                uuid
            );
            return Ok(())
        },
        Err(_) => return Err(Error::CreateTarget{}),
    }
}

/// Undo export of a bdev over iscsi done above.
pub async fn unshare(uuid: &str) -> Result<()> {
    let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();

    info!("Destroying iscsi target {}", iqn);

    unsafe {
        spdk_iscsi_shutdown_tgt_node_by_name( // the name is whatever is int target->name, doesn't have to be iqn
            c_iqn.as_ptr(),
            Some(done_errno_cb),
            cb_arg(sender),
        );
    }
    receiver
        .await
        .expect("Cancellation is not supported")
        .context(DestroyTarget {})?;
    info!("Destroyed iscsi target {}", uuid);
    Ok(())
}

pub fn construct_iscsi_target(bdev_name: &str, pg_idx: c_int, ig_idx: c_int ) -> Result<*mut spdk_iscsi_tgt_node ,Error>{

    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let c_bdev_name = CString::new(bdev_name).unwrap();
    let mut portal_group_idx = pg_idx;
    let mut init_group_idx = ig_idx;

    let mut lun_id: c_int = 0;
    let idx = ISCSI_IDX.with(move |iscsi_idx| {
        let idx = *iscsi_idx.borrow();
        *iscsi_idx.borrow_mut() = idx + 1;
        idx
    });

    let tgt = unsafe {
        spdk_iscsi_tgt_node_construct(
            idx,                             // target_index
            c_iqn.as_ptr(),                  // name
            ptr::null(),                     // alias
            &mut portal_group_idx as *mut _, // pg_tag_list
            &mut init_group_idx as *mut _,   // ig_tag_list
            1,                               // portal and initiator group list length
            &mut c_bdev_name.as_ptr(),       // bdev name, how iscsi target gets associated with storage
            &mut lun_id as *mut _,           // lun id
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
        Err(Error::CreateTarget {})
    } else {
        info!("Created iscsi target {}", iqn);
        Ok(tgt)
    }
}

pub fn init_portal_group(address: &str, port_no: u16, pg_no: c_int) -> Result<()> {
    let portal_port = CString::new(port_no.to_string()).unwrap();
    let portal_host = CString::new(address.to_owned()).unwrap();
    let pg = unsafe { spdk_iscsi_portal_grp_create(pg_no) };
    if pg.is_null() {
        return Err(Error::CreatePortalGroup {});
    }
    unsafe {
        let p = spdk_iscsi_portal_create(
            portal_host.as_ptr(),
            portal_port.as_ptr(),
        );
        if p.is_null() {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::CreatePortal {});
        }
        spdk_iscsi_portal_grp_add_portal(pg, p);
        if spdk_iscsi_portal_grp_open(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::AddPortal {});
        }
        if spdk_iscsi_portal_grp_register(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::RegisterPortalGroup {});
        }
    }
    info!("Created iscsi portal group {}", pg_no);
    Ok(())
}

/// Return iscsi target URI understood by nexus
pub fn get_uri(uuid: &str) -> Option<String> {
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let tgt = unsafe { spdk_iscsi_find_tgt_node(c_iqn.as_ptr()) };

    if tgt.is_null() {
        return None;
    }

    ADDRESS.with(move |a| {
        let a_borrow = a.borrow();
        let address = a_borrow.as_ref().unwrap();
        Some(format!("iscsi://{}:{}/{}", address, ISCSI_PORT_BE, iqn))
    })
}
