// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use nautilus_rithmic::discovery::RithmicDiscoveryCatalog;

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "target/rithmic-diagnostics/rithmic-discovery.json".to_string());
    let query = std::env::args().nth(2);
    let catalog = RithmicDiscoveryCatalog::load(&path)?;

    println!("Entitled exchanges:");
    for exchange in catalog.entitled_exchanges() {
        println!(
            "  {} (L1={}, L2={})",
            exchange.exchange, exchange.level_1_market_data, exchange.level_2_market_data
        );
    }

    let instruments = query.as_deref().map_or_else(
        || catalog.instruments.iter().collect::<Vec<_>>(),
        |query| catalog.find_instruments(query),
    );
    println!("Matching instruments ({}):", instruments.len());
    for instrument in instruments {
        println!(
            "  {}.{}  product={}  type={}  expires={}  {}",
            instrument.exchange,
            instrument.symbol,
            instrument.product_code,
            instrument.instrument_type,
            instrument.expiration_date,
            instrument.symbol_name,
        );
    }
    Ok(())
}
