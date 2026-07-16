use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scid {
    block: u32,
    txindex: u32,
    outnum: u16,
}

impl Scid {
    pub fn block(&self) -> u32 {
        self.block
    }

    pub fn txindex(&self) -> u32 {
        self.txindex
    }

    pub fn outnum(&self) -> u16 {
        self.outnum
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid short_channel_id: {0}")]
pub struct ScidParseError(String);

impl FromStr for Scid {
    type Err = ScidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = if s.contains('x') {
            s.split('x').collect()
        } else {
            s.split(':').collect()
        };
        if parts.len() != 3 {
            return Err(ScidParseError(s.into()));
        }
        let err = || ScidParseError(s.into());
        Ok(Scid {
            block: parts[0].parse().map_err(|_| err())?,
            txindex: parts[1].parse().map_err(|_| err())?,
            outnum: parts[2].parse().map_err(|_| err())?,
        })
    }
}

impl fmt::Display for Scid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x{}x{}", self.block, self.txindex, self.outnum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_x_and_colon_forms() {
        let a: Scid = "931308x1256x1".parse().unwrap();
        let b: Scid = "931308:1256:1".parse().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), "931308x1256x1");
        assert_eq!((a.block(), a.txindex(), a.outnum()), (931308, 1256, 1));
    }

    #[test]
    fn rejects_garbage() {
        assert!("".parse::<Scid>().is_err());
        assert!("1x2".parse::<Scid>().is_err());
        assert!("axbxc".parse::<Scid>().is_err());
    }
}
