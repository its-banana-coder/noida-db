//! SHA-1, for script digests (EVAL, SCRIPT LOAD, `redis.sha1hex`).

/// The SHA-1 digest of `data`, as 40 lowercase hex characters.
pub fn hex(data: &[u8]) -> String {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut msg = data.to_vec();
    let bits = (data.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bits.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let tmp =
                a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        for (slot, v) in h.iter_mut().zip([a, b, c, d, e]) {
            *slot = slot.wrapping_add(v);
        }
    }
    h.iter().map(|w| format!("{w:08x}")).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn known_digests() {
        assert_eq!(super::hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(super::hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(super::hex(b"return 1"), "e0e1f9fabfc9d4800c877a703b823ac0578ff8db");
        // A message long enough to need a second block.
        assert_eq!(super::hex(&b"x".repeat(200)), "94218caae9904e93a3d7bf578bf4791926fc5e82");
    }
}
