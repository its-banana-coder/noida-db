//! `inet` and `cidr`: an IPv4 or IPv6 address with a netmask length.
//! Both types share one value; `cidr` additionally requires the bits
//! right of the netmask to be zero and always prints the `/n`.

use std::cmp::Ordering;
use std::net::{Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Inet {
    pub v6: bool,
    /// The address; IPv4 uses the first 4 bytes.
    pub addr: [u8; 16],
    pub bits: u8,
}

impl Inet {
    pub fn max_bits(&self) -> u8 {
        if self.v6 { 128 } else { 32 }
    }

    fn len(&self) -> usize {
        if self.v6 { 16 } else { 4 }
    }

    pub fn family(&self) -> i64 {
        if self.v6 { 6 } else { 4 }
    }

    fn bit(&self, i: usize) -> bool {
        self.addr[i / 8] & (0x80 >> (i % 8)) != 0
    }

    /// The address with every bit from `bits` on set to `one`.
    fn fill_host(&self, one: bool) -> Inet {
        let mut r = *self;
        for i in self.bits as usize..self.max_bits() as usize {
            let m = 0x80 >> (i % 8);
            if one {
                r.addr[i / 8] |= m;
            } else {
                r.addr[i / 8] &= !m;
            }
        }
        r
    }

    /// The netmask (`ones`) or hostmask as an address.
    fn mask(&self, ones: bool) -> Inet {
        let mut r = Inet { v6: self.v6, addr: [0; 16], bits: self.max_bits() };
        for i in 0..self.max_bits() as usize {
            if (i < self.bits as usize) == ones {
                r.addr[i / 8] |= 0x80 >> (i % 8);
            }
        }
        r
    }

    fn host_bits_zero(&self) -> bool {
        (self.bits as usize..self.max_bits() as usize).all(|i| !self.bit(i))
    }

    fn as_u128(&self) -> u128 {
        let mut b = [0u8; 16];
        let n = self.len();
        b[16 - n..].copy_from_slice(&self.addr[..n]);
        u128::from_be_bytes(b)
    }

    fn from_u128(v6: bool, bits: u8, v: u128) -> Inet {
        let b = v.to_be_bytes();
        let mut addr = [0u8; 16];
        let n = if v6 { 16 } else { 4 };
        addr[..n].copy_from_slice(&b[16 - n..]);
        Inet { v6, addr, bits }
    }

    fn address_text(&self) -> String {
        if self.v6 {
            Ipv6Addr::from(self.addr).to_string()
        } else {
            Ipv4Addr::new(self.addr[0], self.addr[1], self.addr[2], self.addr[3]).to_string()
        }
    }
}

/// Output text: `cidr` always shows `/n`, `inet` only when it isn't a
/// single host.
pub fn format(v: &Inet, cidr: bool) -> String {
    if cidr || v.bits != v.max_bits() {
        format!("{}/{}", v.address_text(), v.bits)
    } else {
        v.address_text()
    }
}

/// `text(inet)` / `inet::text`: always with the netmask.
pub fn format_full(v: &Inet) -> String {
    format!("{}/{}", v.address_text(), v.bits)
}

/// Parses inet/cidr input; `Err` holds Postgres's message.
pub fn parse(s: &str, cidr: bool) -> Result<Inet, String> {
    let ty = if cidr { "cidr" } else { "inet" };
    let bad = || format!("invalid input syntax for type {ty}: \"{s}\"");
    let (addr, bits) = match s.split_once('/') {
        Some((a, b)) => {
            if b.is_empty() || !b.bytes().all(|c| c.is_ascii_digit()) {
                return Err(bad());
            }
            (a, Some(b.parse::<u32>().map_err(|_| bad())?))
        }
        None => (s, None),
    };
    let mut v = if addr.contains(':') {
        let ip: Ipv6Addr = addr.parse().map_err(|_| bad())?;
        Inet { v6: true, addr: ip.octets(), bits: 128 }
    } else {
        let parts: Vec<&str> = addr.split('.').collect();
        let octets: Option<Vec<u8>> = parts
            .iter()
            .map(|p| {
                (!p.is_empty() && p.len() <= 3 && p.bytes().all(|c| c.is_ascii_digit()))
                    .then(|| p.parse::<u8>().ok())
                    .flatten()
            })
            .collect();
        let octets = octets.ok_or_else(bad)?;
        // cidr takes abbreviated networks (`10.1` is 10.1.0.0/16).
        if octets.is_empty() || octets.len() > 4 || (!cidr && octets.len() != 4) {
            return Err(bad());
        }
        let mut addr = [0u8; 16];
        addr[..octets.len()].copy_from_slice(&octets);
        let implied = if octets.len() == 4 { 32 } else { 8 * octets.len() as u8 };
        Inet { v6: false, addr, bits: implied }
    };
    if let Some(b) = bits {
        if b > v.max_bits() as u32 {
            return Err(bad());
        }
        v.bits = b as u8;
    }
    if cidr && !v.host_bits_zero() {
        return Err(format!("invalid cidr value: \"{s}\""));
    }
    Ok(v)
}

/// Postgres's network_cmp: family, the common prefix, the netmask
/// length, then the whole address.
pub fn cmp(a: &Inet, b: &Inet) -> Ordering {
    if a.v6 != b.v6 {
        return a.v6.cmp(&b.v6);
    }
    let common = a.bits.min(b.bits) as usize;
    for i in 0..common {
        match a.bit(i).cmp(&b.bit(i)) {
            Ordering::Equal => {}
            o => return o,
        }
    }
    a.bits.cmp(&b.bits).then_with(|| a.addr[..a.len()].cmp(&b.addr[..b.len()]))
}

/// `a` contains `b` (`>>`; `>>=` with `or_equal`).
pub fn contains(a: &Inet, b: &Inet, or_equal: bool) -> bool {
    if a.v6 != b.v6 || a.bits > b.bits || (!or_equal && a.bits == b.bits) {
        return false;
    }
    (0..a.bits as usize).all(|i| a.bit(i) == b.bit(i))
}

/// `&&`: either contains the other.
pub fn overlaps(a: &Inet, b: &Inet) -> bool {
    contains(a, b, true) || contains(b, a, true)
}

pub fn host(v: &Inet) -> String {
    v.address_text()
}

pub fn network(v: &Inet) -> Inet {
    v.fill_host(false)
}

pub fn broadcast(v: &Inet) -> Inet {
    v.fill_host(true)
}

pub fn netmask(v: &Inet) -> Inet {
    v.mask(true)
}

pub fn hostmask(v: &Inet) -> Inet {
    v.mask(false)
}

/// `abbrev`: cidr drops trailing zero octets an IPv4 netmask covers.
pub fn abbrev(v: &Inet, cidr: bool) -> String {
    if !cidr {
        return format(v, false);
    }
    if v.v6 {
        return format(v, true);
    }
    let keep = (v.bits as usize).div_ceil(8).max(1);
    let octets: Vec<String> = v.addr[..keep].iter().map(|o| o.to_string()).collect();
    format!("{}/{}", octets.join("."), v.bits)
}

pub fn set_masklen(v: &Inet, bits: i64, cidr: bool) -> Result<Inet, String> {
    let bits = if bits == -1 { v.max_bits() as i64 } else { bits };
    if bits < 0 || bits > v.max_bits() as i64 {
        return Err(format!("invalid mask length: {bits}"));
    }
    let r = Inet { bits: bits as u8, ..*v };
    Ok(if cidr { r.fill_host(false) } else { r })
}

/// `inet_merge`: the smallest network holding both.
pub fn merge(a: &Inet, b: &Inet) -> Result<Inet, String> {
    if a.v6 != b.v6 {
        return Err("cannot merge addresses from different families".into());
    }
    let mut bits = a.bits.min(b.bits);
    for i in 0..bits as usize {
        if a.bit(i) != b.bit(i) {
            bits = i as u8;
            break;
        }
    }
    Ok(Inet { bits, ..*a }.fill_host(false))
}

/// `inet + n` / `inet - n`.
pub fn add(v: &Inet, n: i128) -> Result<Inet, String> {
    let max: u128 = if v.v6 { u128::MAX } else { u32::MAX as u128 };
    let cur = v.as_u128() as i128;
    let r = cur.checked_add(n).filter(|r| *r >= 0 && (*r as u128) <= max);
    match r {
        Some(r) => Ok(Inet::from_u128(v.v6, v.bits, r as u128)),
        None => Err("result is out of range".into()),
    }
}

/// `inet - inet`: the difference of the addresses.
pub fn sub(a: &Inet, b: &Inet) -> Result<i64, String> {
    if a.v6 != b.v6 {
        return Err("cannot subtract inet values of different sizes".into());
    }
    let d = a.as_u128() as i128 - b.as_u128() as i128;
    i64::try_from(d).map_err(|_| "result is out of range".into())
}

/// Binary wire format: family, bits, is_cidr, length, address bytes.
pub fn to_binary(v: &Inet, cidr: bool) -> Vec<u8> {
    let mut out = vec![if v.v6 { 3 } else { 2 }, v.bits, u8::from(cidr), v.len() as u8];
    out.extend_from_slice(&v.addr[..v.len()]);
    out
}

pub fn from_binary(b: &[u8]) -> Option<Inet> {
    let (&family, rest) = b.split_first()?;
    let v6 = match family {
        2 => false,
        3 => true,
        _ => return None,
    };
    let bits = *rest.first()?;
    let n = *rest.get(2)? as usize;
    let bytes = rest.get(3..3 + n)?;
    if n != if v6 { 16 } else { 4 } {
        return None;
    }
    let mut addr = [0u8; 16];
    addr[..n].copy_from_slice(bytes);
    Some(Inet { v6, addr, bits })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format() {
        let p = |s: &str, c: bool| parse(s, c).map(|v| format(&v, c));
        assert_eq!(p("192.168.1.5/32", false).unwrap(), "192.168.1.5");
        assert_eq!(p("192.168.1.5/24", false).unwrap(), "192.168.1.5/24");
        assert_eq!(p("10.1", true).unwrap(), "10.1.0.0/16");
        assert!(p("192.168.1.5/24", true).unwrap_err().starts_with("invalid cidr value"));
        assert!(p("192.168", false).is_err());
        assert!(p("1.2.3.4/33", false).is_err());
        assert_eq!(
            p("2001:0DB8:0000:0000:0000:0000:0000:0001/64", false).unwrap(),
            "2001:db8::1/64"
        );
        assert_eq!(p("1:0:0:2:0:0:3:4", false).unwrap(), "1::2:0:0:3:4");
    }

    #[test]
    fn ordering_and_ops() {
        let i = |s: &str| parse(s, false).unwrap();
        let mut v = [
            i("10.0.0.10"),
            i("10.0.0.2"),
            i("::1"),
            i("10.0.0.2/8"),
            i("10.0.0.0/8"),
            i("9.255.255.255"),
        ];
        v.sort_by(cmp);
        let out: Vec<String> = v.iter().map(|x| format(x, false)).collect();
        assert_eq!(
            out,
            ["9.255.255.255", "10.0.0.0/8", "10.0.0.2/8", "10.0.0.2", "10.0.0.10", "::1"]
        );
        assert!(contains(&i("10.0.0.0/8"), &i("10.1.2.3"), false));
        assert_eq!(
            format(&merge(&i("192.168.1.5/24"), &i("192.168.2.5/24")).unwrap(), true),
            "192.168.0.0/22"
        );
        assert_eq!(format(&add(&i("192.168.1.5"), 300).unwrap(), false), "192.168.2.49");
        assert_eq!(abbrev(&parse("10.0.0.0/8", true).unwrap(), true), "10/8");
        assert_eq!(
            format(&broadcast(&i("2001:db8::1/64")), false),
            "2001:db8::ffff:ffff:ffff:ffff/64"
        );
    }
}
