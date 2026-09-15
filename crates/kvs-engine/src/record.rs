use crate::{Command, EngineError, Result};

pub const MAX_KEY_BYTES: usize = 1_024; // 1 KiB
pub const MAX_VALUE_BYTES: usize = 1_048_576; // 1 MiB
// Add 64 bytes of wiggle room
pub(crate) const MAX_PAYLOAD_BYTES: usize = MAX_KEY_BYTES + MAX_VALUE_BYTES + 64;
pub(crate) const HEADER_LEN: usize = size_of::<u32>() * 2;

pub(crate) fn encode(cmd: &Command) -> Result<Vec<u8>> {
    // Message structure:
    // [ crc - 4b ][ len - 4b ][ payload - <len>b ]
    // CRC = cyclic redundancy check which catches accidental corruption
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard())
        .map_err(|e| EngineError::Encode(e.to_string()))?;
    let crc = crc32fast::hash(&payload);
    let len = u32::try_from(payload.len()).expect("payload.len() exceeds u32");

    // Build byte buffer and return
    let mut buffer = Vec::with_capacity(HEADER_LEN + payload.len());
    buffer.extend_from_slice(&crc.to_le_bytes());
    buffer.extend_from_slice(&len.to_le_bytes());
    buffer.extend_from_slice(&payload);
    Ok(buffer)
}

pub(crate) fn decode(buffer: &[u8], offset: u64) -> Result<Command> {
    // Validate minimum length
    if buffer.len() < HEADER_LEN {
        return Err(EngineError::Corrupt { offset });
    }

    let header: [u8; HEADER_LEN] = buffer[..HEADER_LEN]
        .try_into()
        .expect("length checked above");

    // Compare header-declared length to actual buffer length
    if buffer.len() - HEADER_LEN != payload_len(header) as usize {
        return Err(EngineError::Corrupt { offset });
    }

    let payload = &buffer[HEADER_LEN..];
    let payload_hash = crc32fast::hash(payload);

    // Make sure payload hash matches header CRC
    if payload_hash != stored_crc(header) {
        return Err(EngineError::Corrupt { offset });
    }

    let (cmd, _consumed) =
        bincode::serde::decode_from_slice::<Command, _>(payload, bincode::config::standard())
            .map_err(|_err| EngineError::Corrupt { offset })?;
    Ok(cmd)
}

fn stored_crc(header: [u8; HEADER_LEN]) -> u32 {
    u32::from_le_bytes(
        header[..4]
            .try_into()
            .expect("a 4-byte window of a fixed array"),
    )
}

pub(crate) fn payload_len(header: [u8; HEADER_LEN]) -> u32 {
    u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("a 4-byte window of a fixed array"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, EngineError};

    fn sample() -> Command {
        Command::Set {
            key: "alpha".into(),
            value: "one".into(),
        }
    }

    #[test]
    fn a_record_round_trips() {
        let cmd = sample();
        let bytes = encode(&cmd).expect("encode");
        assert_eq!(decode(&bytes, 0).expect("decode"), cmd);
    }

    #[test]
    fn a_remove_record_round_trips() {
        let cmd = Command::Remove {
            key: "alpha".into(),
        };
        let bytes = encode(&cmd).expect("encode");
        assert_eq!(decode(&bytes, 0).expect("decode"), cmd);
    }

    #[test]
    fn the_header_reports_the_payload_length() {
        let bytes = encode(&sample()).expect("encode");
        let header: [u8; HEADER_LEN] = bytes[..HEADER_LEN].try_into().expect("header");
        assert_eq!(payload_len(header) as usize, bytes.len() - HEADER_LEN);
    }

    #[test]
    fn a_tampered_payload_is_rejected() {
        let mut bytes = encode(&sample()).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the payload, leave the checksum intact
        let err = decode(&bytes, 4096).expect_err("a bad checksum must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 4096 }));
    }

    #[test]
    fn a_record_shorter_than_its_header_claims_is_rejected() {
        let bytes = encode(&sample()).expect("encode");
        let truncated = &bytes[..bytes.len() - 1];
        let err = decode(truncated, 0).expect_err("a short record must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 0 }));
    }

    #[test]
    fn a_buffer_smaller_than_a_header_is_rejected() {
        let err = decode(&[0u8; 3], 0).expect_err("a runt buffer must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 0 }));
    }
}
