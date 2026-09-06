use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

const MEV_BOT_STR_ID: &str = "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz";
const NA247_STR_ID: &str = "NA247a7YE9S3p9CdKmMyETx8TTwbSdVbVYHHxpnHTUV";
const NA365_STR_ID: &str = "NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5";
const J9_STR_ID: &str = "J9G2mzdy3vrgY25GQA1pbysvNNtbnM4mQB2jH4tWT3Mx";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum StaticProgramLog {
    SolanaMevBotCom,
    NoProfitableArbitrageFound,
    NoProfitableArbitrageOpportunityFound,
    NoArbitrageProfitFound,
    MoneyBag,
    NoArbitrageProfitFoundIndex(u64),
}

impl StaticProgramLog {
    pub fn parse(program: &str, payload: &str) -> Option<Self> {
        if matches!(program, NA247_STR_ID | NA365_STR_ID)
            && let Some(value) = parse_no_arbitrage_profit_found_index(payload)
        {
            return Some(Self::NoArbitrageProfitFoundIndex(value));
        }

        match (program, payload) {
            (MEV_BOT_STR_ID, "SolanaMevBot.com") => Some(Self::SolanaMevBotCom),
            (MEV_BOT_STR_ID, "No profitable arbitrage found") => {
                Some(Self::NoProfitableArbitrageFound)
            }
            (MEV_BOT_STR_ID, "No profitable arbitrage opportunity found") => {
                Some(Self::NoProfitableArbitrageOpportunityFound)
            }
            (NA247_STR_ID | NA365_STR_ID, "No arbitrage profit found!") => {
                Some(Self::NoArbitrageProfitFound)
            }
            (J9_STR_ID, "🤑") => Some(Self::MoneyBag),
            _ => None,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::SolanaMevBotCom => "SolanaMevBot.com",
            Self::NoProfitableArbitrageFound => "No profitable arbitrage found",
            Self::NoProfitableArbitrageOpportunityFound => {
                "No profitable arbitrage opportunity found"
            }
            Self::NoArbitrageProfitFound => "No arbitrage profit found!",
            Self::MoneyBag => "🤑",
            Self::NoArbitrageProfitFoundIndex(index) => {
                return format!("No arbitrage profit found! ({index})");
            }
        }
        .to_string()
    }
}

fn parse_no_arbitrage_profit_found_index(payload: &str) -> Option<u64> {
    let rest = payload.strip_prefix("No arbitrage profit found! (")?;
    rest.strip_suffix(')')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_program_logs_round_trip() {
        let log = StaticProgramLog::parse(NA247_STR_ID, "No arbitrage profit found! (0)").unwrap();
        assert_eq!(log.as_str(), "No arbitrage profit found! (0)");

        let log = StaticProgramLog::parse(NA247_STR_ID, "No arbitrage profit found!").unwrap();
        assert_eq!(log.as_str(), "No arbitrage profit found!");
    }
}
