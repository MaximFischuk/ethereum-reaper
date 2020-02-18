use serde::{Deserialize, Deserializer};
use web3::types::U64;

pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<U64, D::Error>
    where
        D: Deserializer<'de>,
{
    u64::deserialize(deserializer).map(|v| U64::from(v))
}

pub mod selector {
    use serde::{Deserialize, Deserializer};
    use jsonpath::Selector;

    pub fn deserialize<'de, D>(
        deserializer: D
    ) -> Result<Selector, D::Error>
        where
            D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|v| Selector::new(v.as_str()).unwrap())
    }
}