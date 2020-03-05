use byte_unit::Byte;
use rpc::mayastor::ShareProtocol;

/// converts a human string into a blocklen
#[allow(dead_code)]
pub(crate) fn parse_block_len(src: &str) -> Result<u32, String> {
    if let Ok(val) = Byte::from_str(src) {
        let val = val.get_bytes() as u32;
        if !val.is_power_of_two() {
            Err(format!("{} is not a power of two", val))
        } else {
            Ok(val)
        }
    } else {
        Err(format!("Invalid argument {}", src))
    }
}

/// parses a human string into bytes accounts for MiB and MB
pub(crate) fn parse_size(src: &str) -> Result<u64, String> {
    if let Ok(val) = Byte::from_str(src) {
        Ok(val.get_bytes() as u64)
    } else {
        Err(src.to_string())
    }
}

pub (crate) fn parse_proto(src: &str) -> Result<ShareProtocol, &str> {
    match src.to_lowercase().trim() {
        "nvmf" => Ok(ShareProtocol::Nvmf),
        "iscsi" => Ok(ShareProtocol::Iscsi),
        "nbd" => Ok(ShareProtocol::Nbd),
        _ => Err("Protocol needs be either NVMf, ISCI or NBD"),
    }
}