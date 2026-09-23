//! Password authentication: cleartext, md5 and SCRAM-SHA-256, with the
//! small hash functions they need (no crypto dependencies).

/// How the server asks clients to authenticate.
#[derive(Clone, Debug, PartialEq)]
pub enum AuthMethod {
    /// Accept everyone (the default, like a local `trust` pg_hba line).
    Trust,
    Password,
    Md5,
    ScramSha256,
}

impl AuthMethod {
    pub fn parse(s: &str) -> Option<AuthMethod> {
        Some(match s.to_ascii_lowercase().as_str() {
            "trust" => AuthMethod::Trust,
            "password" | "cleartext" => AuthMethod::Password,
            "md5" => AuthMethod::Md5,
            "scram" | "scram-sha-256" => AuthMethod::ScramSha256,
            _ => return None,
        })
    }
}

// ---------------------------------------------------------------------------
// MD5 (RFC 1321)

pub fn md5(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    let k: Vec<u32> =
        (0..64).map(|i| ((i as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32).collect();
    let mut a0: u32 = 0x6745_2301;
    let mut b0: u32 = 0xefcd_ab89;
    let mut c0: u32 = 0x98ba_dcfe;
    let mut d0: u32 = 0x1032_5476;
    let mut msg = input.to_vec();
    let bit_len = (input.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());
    for chunk in msg.chunks(64) {
        let m: Vec<u32> =
            chunk.chunks(4).map(|w| u32::from_le_bytes(w.try_into().unwrap())).collect();
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f2 = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f2.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    out[..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..].copy_from_slice(&d0.to_le_bytes());
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// What an md5 client sends: `md5` + md5(md5(password + user) + salt).
pub fn md5_response(user: &str, password: &str, salt: [u8; 4]) -> String {
    let inner = hex(&md5(format!("{password}{user}").as_bytes()));
    let mut buf = inner.into_bytes();
    buf.extend_from_slice(&salt);
    format!("md5{}", hex(&md5(&buf)))
}

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4), HMAC, PBKDF2

pub fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = input.to_vec();
    let bit_len = (input.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let mut v = h;
        for i in 0..64 {
            let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
            let ch = (v[4] & v[5]) ^ (!v[4] & v[6]);
            let t1 = v[7].wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
            let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
            let t2 = s0.wrapping_add(maj);
            v = [t1.wrapping_add(t2), v[0], v[1], v[2], v[3].wrapping_add(t1), v[4], v[5], v[6]];
        }
        for i in 0..8 {
            h[i] = h[i].wrapping_add(v[i]);
        }
    }
    let mut out = [0u8; 32];
    for (i, x) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&x.to_be_bytes());
    }
    out
}

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner: Vec<u8> = k.iter().map(|b| b ^ 0x36).collect();
    inner.extend_from_slice(msg);
    let ih = sha256(&inner);
    let mut outer: Vec<u8> = k.iter().map(|b| b ^ 0x5c).collect();
    outer.extend_from_slice(&ih);
    sha256(&outer)
}

/// SCRAM's Hi(): PBKDF2-HMAC-SHA-256 with one output block.
pub fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut s = salt.to_vec();
    s.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password, &s);
    let mut out = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (o, x) in out.iter_mut().zip(u.iter()) {
            *o ^= x;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 server side

pub const SCRAM_ITERATIONS: u32 = 4096;

pub struct Scram {
    password: String,
    salt: Vec<u8>,
    nonce: String,
    client_first_bare: String,
    server_first: String,
}

#[derive(Debug, PartialEq)]
pub enum ScramError {
    Protocol(String),
    BadPassword,
}

fn attr(msg: &str, key: char) -> Option<&str> {
    msg.split(',').find_map(|p| p.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
}

impl Scram {
    pub fn new(password: &str) -> Scram {
        let mut salt = vec![0u8; 16];
        for chunk in salt.chunks_mut(8) {
            chunk.copy_from_slice(&super::funcs::random_u64().to_le_bytes()[..chunk.len()]);
        }
        Scram {
            password: password.to_string(),
            salt,
            nonce: String::new(),
            client_first_bare: String::new(),
            server_first: String::new(),
        }
    }

    /// Handles client-first-message; returns server-first-message.
    pub fn client_first(&mut self, msg: &str) -> Result<String, ScramError> {
        let bad = |m: &str| ScramError::Protocol(m.to_string());
        let (gs2, bare) = match msg.find(",,") {
            Some(i) => (&msg[..i + 2], &msg[i + 2..]),
            None => return Err(bad("malformed SCRAM message")),
        };
        if gs2.starts_with('p') {
            return Err(bad("channel binding is not supported"));
        }
        let cnonce = attr(bare, 'r').ok_or_else(|| bad("malformed SCRAM message"))?;
        let mut snonce = [0u8; 18];
        for chunk in snonce.chunks_mut(8) {
            chunk.copy_from_slice(&super::funcs::random_u64().to_le_bytes()[..chunk.len()]);
        }
        self.nonce = format!("{cnonce}{}", super::funcs::base64_encode(&snonce));
        self.client_first_bare = bare.to_string();
        self.server_first = format!(
            "r={},s={},i={}",
            self.nonce,
            super::funcs::base64_encode(&self.salt),
            SCRAM_ITERATIONS
        );
        Ok(self.server_first.clone())
    }

    /// Handles client-final-message; returns server-final-message.
    pub fn client_final(&self, msg: &str) -> Result<String, ScramError> {
        let bad = |m: &str| ScramError::Protocol(m.to_string());
        let nonce = attr(msg, 'r').ok_or_else(|| bad("malformed SCRAM message"))?;
        if nonce != self.nonce {
            return Err(bad("unexpected SCRAM nonce"));
        }
        let proof_b64 = attr(msg, 'p').ok_or_else(|| bad("malformed SCRAM message"))?;
        let proof =
            super::funcs::base64_decode(proof_b64).ok_or_else(|| bad("malformed SCRAM message"))?;
        let without_proof =
            &msg[..msg.rfind(",p=").ok_or_else(|| bad("malformed SCRAM message"))?];
        let auth_message =
            format!("{},{},{}", self.client_first_bare, self.server_first, without_proof);
        let salted = pbkdf2(self.password.as_bytes(), &self.salt, SCRAM_ITERATIONS);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        if proof.len() != 32 {
            return Err(ScramError::BadPassword);
        }
        let recovered: Vec<u8> = proof.iter().zip(signature.iter()).map(|(p, s)| p ^ s).collect();
        if sha256(&recovered) != stored_key {
            return Err(ScramError::BadPassword);
        }
        let server_key = hmac_sha256(&salted, b"Server Key");
        let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        Ok(format!("v={}", super::funcs::base64_encode(&server_sig)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests() {
        assert_eq!(hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            hex(&md5(b"The quick brown fox jumps over the lazy dog")),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&hmac_sha256(b"key", b"The quick brown fox jumps over the lazy dog")),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn scram_roundtrip() {
        // Play the client side with the same primitives.
        let mut s = Scram::new("pencil");
        let client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let sf = s.client_first(client_first).unwrap();
        let nonce = attr(&sf, 'r').unwrap().to_string();
        let salt = super::super::funcs::base64_decode(attr(&sf, 's').unwrap()).unwrap();
        let salted = pbkdf2(b"pencil", &salt, 4096);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored = sha256(&client_key);
        let without = format!("c=biws,r={nonce}");
        let auth = format!("n=user,r=rOprNGfwEbeRWgbNEkqO,{sf},{without}");
        let sig = hmac_sha256(&stored, auth.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(sig.iter()).map(|(a, b)| a ^ b).collect();
        let final_msg = format!("{without},p={}", super::super::funcs::base64_encode(&proof));
        assert!(s.client_final(&final_msg).unwrap().starts_with("v="));
        let bad = format!("{without},p={}", super::super::funcs::base64_encode(&[0u8; 32]));
        assert_eq!(s.client_final(&bad), Err(ScramError::BadPassword));
    }
}
