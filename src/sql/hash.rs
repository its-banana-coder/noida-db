//! Message digests and HMACs by algorithm name: pgcrypto's `digest()` /
//! `hmac()` and MySQL's `SHA1()` / `SHA2()`.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Digest;

/// The digest of `data`, or `None` for an algorithm we don't know.
/// Names are matched case-insensitively, as pgcrypto does.
pub fn digest(alg: &str, data: &[u8]) -> Option<Vec<u8>> {
    Some(match alg.to_ascii_lowercase().as_str() {
        "md5" => md5::Md5::digest(data).to_vec(),
        "sha1" => sha1::Sha1::digest(data).to_vec(),
        "sha224" => sha2::Sha224::digest(data).to_vec(),
        "sha256" => sha2::Sha256::digest(data).to_vec(),
        "sha384" => sha2::Sha384::digest(data).to_vec(),
        "sha512" => sha2::Sha512::digest(data).to_vec(),
        _ => return None,
    })
}

/// HMAC of `data` under `key`, or `None` for an unknown algorithm.
pub fn hmac(alg: &str, key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    fn run<M: Mac + KeyInit>(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut m = <M as KeyInit>::new_from_slice(key).expect("HMAC takes any key length");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    }
    Some(match alg.to_ascii_lowercase().as_str() {
        "md5" => run::<Hmac<md5::Md5>>(key, data),
        "sha1" => run::<Hmac<sha1::Sha1>>(key, data),
        "sha224" => run::<Hmac<sha2::Sha224>>(key, data),
        "sha256" => run::<Hmac<sha2::Sha256>>(key, data),
        "sha384" => run::<Hmac<sha2::Sha384>>(key, data),
        "sha512" => run::<Hmac<sha2::Sha512>>(key, data),
        _ => return None,
    })
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Base64, a newline after every 76 characters (Postgres `encode()`
/// and MySQL `TO_BASE64()` both wrap there).
pub fn base64_encode(b: &[u8]) -> String {
    let mut out = String::new();
    for (n, chunk) in b.chunks(3).enumerate() {
        if n > 0 && n % 19 == 0 {
            out.push('\n');
        }
        let v = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(B64[(v >> 18) as usize & 63] as char);
        out.push(B64[(v >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(v >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[v as usize & 63] as char } else { '=' });
    }
    out
}

pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = vec![];
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == b'=' {
            break;
        }
        let v = B64.iter().position(|&x| x == c)? as u32;
        buf = buf << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(
            hex(&digest("SHA1", b"abc").unwrap()),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            hex(&digest("sha224", b"abc").unwrap()),
            "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
        );
        assert_eq!(
            hex(&hmac("sha256", b"key", b"abc").unwrap()),
            "9c196e32dc0175f86f4b1cb89289d6619de6bee699e4c378e68309ed97a1a6ab"
        );
        assert!(digest("nope", b"").is_none());
    }
}
