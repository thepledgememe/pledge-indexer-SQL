require('dotenv').config();
const axios = require('axios');
const mysql = require('mysql2');
/**
 * -------------------------DEV NOTES-------------------------
 * This indexer is the firs titeration of the pledge tracking system.
 * this system will check all blocks from launch of PLEDGE current blocks are stored in metadata table
 * this system then check every blocks logs for a transfer event and a pledge event.
 * if a transfer event is found it will update the holders table with the new balance.
 * if a pledge event is found it will update the pledgers table with the new pledge amount.
 * if a broken pledge event is found it will update the pledgers table with the new status and broken timestamp.
 * if the from address is the zero address it will update the pledgers table with the new pledge amount (as this is considered part of the airdorp event)
 * if a transfer event is found with the burn address as the to address it will update the burned tokens in the metadata table.
 * if a transfer event is found with the community wallet or liquidity pool as the to address it will update the metadata table with the new balance.
 * 
 * Please note the following IS required to run this bot:
 * SQL DB
 * ENV Variables
 * - DB_HOST
 * - DB_USER
 * - DB_PASSWORD
 * - DB_NAME
 * - ALCHEMY_API_KEY
 * - CONTRACT_ADDRESS
 * 
 * The following is optional:
 * - COMMUNITY_WALLET
 * - LIQUIDITY_POOL
 * 
 * 
 * -------------------------Initial Concept by https://github.com/ChrisLivermore -------------------------
 */





const SQLPromisePool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
}).promise();

const ALCHEMY_API_URL = `https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;

const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const CREATED_PLEDGE_TOPIC = "0x8befdfb767f1fa8afaab1250205358276a8dfd21b2d744e0c068ec9ce67f70e2";
const BROKEN_PLEDGE_TOPIC = "0xf3baa52f900a58295c5dcdd74f103ba109926ee544d3fc43547540aa482fadbf";

const COMMUNITY_WALLET = "0x609adc791615f48e0dbd247357cecd1c0ccc4bbf";
const LIQUIDITY_POOL = "0xc3a4f2231453535fa564508eb6828bd449705082";

const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
const BURN_ADDRESS = "0x000000000000000000000000000000000000dead";

const CONTRACT_CREATION_BLOCK = 21344733;
const AIRDROP_AMOUNT = BigInt("1000000000000000000000000");

const blockTimestampCache = new Map();

function formatWeiToDecimal(weiValue) {
    const strVal = weiValue.toString().padStart(19, '0');
    const intPart = strVal.slice(0, strVal.length - 18);
    const fracPart = strVal.slice(-18);
    return `${intPart}.${fracPart}`;
}

function decimalToWeiBigInt(decimalStr) {
    let parts = decimalStr.split('.');
    let whole = parts[0];
    let frac = parts[1] || '';
    frac = frac.padEnd(18, '0');
    return BigInt(whole + frac);
}

async function createTables() {
    const queries = [
        `CREATE TABLE IF NOT EXISTS holders (
            address VARCHAR(42) PRIMARY KEY,
            balance DECIMAL(65,18) NOT NULL DEFAULT 0
        )`,
        `CREATE TABLE IF NOT EXISTS transactions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tx_hash VARCHAR(66) NOT NULL,
            block_number BIGINT NOT NULL,
            from_address VARCHAR(42),
            to_address VARCHAR(42),
            amount DECIMAL(65,18) NOT NULL,
            timestamp BIGINT NOT NULL,
            INDEX (tx_hash)
        )`,
        `CREATE TABLE IF NOT EXISTS metadata (
            key_name VARCHAR(50) PRIMARY KEY,
            key_value VARCHAR(255)
        )`,
        `CREATE TABLE IF NOT EXISTS pledgers (
            address VARCHAR(42) PRIMARY KEY,
            is_og TINYINT(1) DEFAULT 0,
            pledged_amount DECIMAL(65,18) DEFAULT 0,
            token_balance DECIMAL(65,18) DEFAULT 0,
            status TINYINT DEFAULT 0,
            last_updated BIGINT NOT NULL,
            pledge_timestamp BIGINT DEFAULT NULL,
            broken_timestamp BIGINT DEFAULT NULL,
            broken_hash VARCHAR(66) DEFAULT NULL
        )`,
        `CREATE TABLE IF NOT EXISTS pledge_history (
            timestamp BIGINT NOT NULL,
            total_pledged DECIMAL(65,18),
            pledged INT DEFAULT 0,
            broken_pledge INT DEFAULT 0,
            PRIMARY KEY (timestamp)
        )`
    ];
    for (const query of queries) {
        await SQLPromisePool.execute(query);
    }
    console.log("Tables are ready.");
}

async function getLastProcessedBlock() {
    const [rows] = await SQLPromisePool.execute(`SELECT key_value FROM metadata WHERE key_name = 'last_processed_block'`);
    return rows.length > 0 ? parseInt(rows[0].key_value, 10) : null;
}

async function updateLastProcessedBlock(blockNumber) {
    await SQLPromisePool.execute(
        `INSERT INTO metadata (key_name, key_value) VALUES ('last_processed_block', ?)
         ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)`,
        [blockNumber.toString()]
    );
}

async function getLatestBlockNumber() {
    const params = { jsonrpc: "2.0", id: 1, method: "eth_blockNumber", params: [] };
    const response = await axios.post(ALCHEMY_API_URL, params, { headers: { "Content-Type": "application/json" }});
    return response.data.result;
}

async function getBlockTimestamp(blockNumber) {
    if (blockTimestampCache.has(blockNumber)) {
        return blockTimestampCache.get(blockNumber);
    }
    const hexBlock = "0x" + blockNumber.toString(16);
    const params = { jsonrpc: "2.0", id: 1, method: "eth_getBlockByNumber", params: [hexBlock, false] };
    const response = await axios.post(ALCHEMY_API_URL, params, { headers: { "Content-Type": "application/json" } });
    const blockData = response.data.result;
    const blockTimestamp = parseInt(blockData.timestamp, 16);
    blockTimestampCache.set(blockNumber, blockTimestamp);
    return blockTimestamp;
}

async function getTotalPledged() {
    const [rows] = await SQLPromisePool.execute(`SELECT key_value FROM metadata WHERE key_name = 'total_pledged'`);
    return rows.length > 0 ? rows[0].key_value : "0";
}

async function setTotalPledged(value) {
    await SQLPromisePool.execute(`
        INSERT INTO metadata (key_name, key_value) VALUES ('total_pledged', ?)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)
    `, [value.toString()]);
}

async function countPledgedWallets() {
    const [[pledgedCount]] = await SQLPromisePool.execute(`SELECT COUNT(*) as c FROM pledgers WHERE status = 1`);
    return pledgedCount.c;
}

async function countBrokenWallets() {
    const [[brokenCount]] = await SQLPromisePool.execute(`SELECT COUNT(*) as c FROM pledgers WHERE status = 2`);
    return brokenCount.c;
}

async function insertOrUpdatePledgeHistory(timestamp, totalDecimal) {
    const pledgedCount = await countPledgedWallets();
    const brokenCount = await countBrokenWallets();
    await SQLPromisePool.execute(`
        INSERT INTO pledge_history (timestamp, total_pledged, pledged, broken_pledge) VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE total_pledged = VALUES(total_pledged),
                                pledged = VALUES(pledged),
                                broken_pledge = VALUES(broken_pledge)
    `, [timestamp, totalDecimal, pledgedCount, brokenCount]);
}

async function fillMissingSeconds(lastTimestamp, currentTimestamp, totalDecimal) {
    const pledgedCount = await countPledgedWallets();
    const brokenCount = await countBrokenWallets();
    for (let t = lastTimestamp + 1; t < currentTimestamp; t++) {
        await SQLPromisePool.execute(`
            INSERT INTO pledge_history (timestamp, total_pledged, pledged, broken_pledge)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE total_pledged = VALUES(total_pledged),
                                    pledged = VALUES(pledged),
                                    broken_pledge = VALUES(broken_pledge)
        `, [t, totalDecimal, pledgedCount, brokenCount]);
    }
}

async function adjustTotalPledged(delta, blockTimestamp) {
    const currentTotalDecimal = await getTotalPledged();
    const currentTotalWei = decimalToWeiBigInt(currentTotalDecimal);
    const newTotalWei = currentTotalWei + delta;
    const newTotalDecimal = formatWeiToDecimal(newTotalWei);
    const [[lastEntry]] = await SQLPromisePool.execute(
        `SELECT timestamp, total_pledged FROM pledge_history ORDER BY timestamp DESC LIMIT 1`
    );
    if (lastEntry) {
        const lastTimestamp = parseInt(lastEntry.timestamp, 10);
        if (blockTimestamp > lastTimestamp + 1) {
            await fillMissingSeconds(lastTimestamp, blockTimestamp, lastEntry.total_pledged);
        }
    }
    await setTotalPledged(newTotalDecimal);
    await insertOrUpdatePledgeHistory(blockTimestamp, newTotalDecimal);
    console.log(`Total pledged adjusted by ${delta.toString()}, new total: ${newTotalDecimal}`);
}

async function upsertPledgerFinal(address, isOG, pledgedAmountFinal, tokenBalanceDelta, statusUpdate, blockTimestamp, setPledgeTimestamp=false, brokenTimestamp=null, brokenHash=null) {
    // This function sets the final pledged_amount directly, not adjusting total_pledged inside
    if (address.toLowerCase() === ZERO_ADDRESS || address.toLowerCase() === BURN_ADDRESS) {
        return;
    }

    const [[existing]] = await SQLPromisePool.execute(`SELECT * FROM pledgers WHERE address = ?`, [address]);
    let currentBalance = BigInt(0);
    let currentIsOg = 0;
    let currentStatus = 0;
    let currentPledgeTimestamp = null;
    if (existing) {
        currentBalance = decimalToWeiBigInt(existing.token_balance.toString());
        currentIsOg = existing.is_og;
        currentStatus = existing.status;
        currentPledgeTimestamp = existing.pledge_timestamp;
    }

    const newIsOG = (isOG !== null) ? isOG : currentIsOg;
    let finalStatus = (statusUpdate !== null) ? statusUpdate : currentStatus;

    let newBalance = currentBalance + tokenBalanceDelta;
    if (newBalance < 0) {
        newBalance = BigInt(0);
    }

    let pledgeTimestampValue = currentPledgeTimestamp;
    if (setPledgeTimestamp && pledgeTimestampValue == null) {
        pledgeTimestampValue = blockTimestamp;
    }

    const pledgedAmountDecimal = formatWeiToDecimal(pledgedAmountFinal);
    const tokenBalanceDecimal = formatWeiToDecimal(newBalance);

    // broken_timestamp and broken_hash updates only if finalStatus=2
    let brokenTimestampSQL = brokenTimestamp !== null && finalStatus === 2 ? ', broken_timestamp = VALUES(broken_timestamp)' : '';
    let brokenHashSQL = brokenHash !== null && finalStatus === 2 ? ', broken_hash = VALUES(broken_hash)' : '';

    await SQLPromisePool.execute(`
        INSERT INTO pledgers (address, is_og, pledged_amount, token_balance, status, last_updated, pledge_timestamp, broken_timestamp, broken_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            is_og = VALUES(is_og),
            pledged_amount = VALUES(pledged_amount),
            token_balance = VALUES(token_balance),
            status = VALUES(status),
            last_updated = VALUES(last_updated)
            ${pledgeTimestampValue === currentPledgeTimestamp ? '' : ', pledge_timestamp = VALUES(pledge_timestamp)'}
            ${brokenTimestampSQL}
            ${brokenHashSQL}
    `, [address, newIsOG, pledgedAmountDecimal, tokenBalanceDecimal, finalStatus, blockTimestamp, pledgeTimestampValue, brokenTimestamp, brokenHash]);
}

async function updateHolderBalance(address, amountDecimal) {
    if (address.toLowerCase() === ZERO_ADDRESS || address.toLowerCase() === BURN_ADDRESS) {
        return;
    }
    await SQLPromisePool.execute(`
        INSERT INTO holders (address, balance) VALUES (?, ?)
        ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)
    `, [address, amountDecimal]);
}

async function increaseBurnedTokens(amountDecimal) {
    const [rows] = await SQLPromisePool.execute(`SELECT key_value FROM metadata WHERE key_name = 'burned_tokens'`);
    let currentBurned = "0";
    if (rows.length > 0) {
        currentBurned = rows[0].key_value;
    }
    const currentBurnedWei = decimalToWeiBigInt(currentBurned);
    const addWei = decimalToWeiBigInt(amountDecimal);
    const newWei = currentBurnedWei + addWei;
    const newDecimal = formatWeiToDecimal(newWei);

    await SQLPromisePool.execute(`
        INSERT INTO metadata (key_name, key_value) VALUES ('burned_tokens', ?)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)
    `, [newDecimal.toString()]);
}

async function updateSpecialHolders() {
    const [[cw]] = await SQLPromisePool.execute(`SELECT balance FROM holders WHERE address = ?`, [COMMUNITY_WALLET]);
    const communityBalance = cw ? cw.balance : "0";

    const [[lp]] = await SQLPromisePool.execute(`SELECT balance FROM holders WHERE address = ?`, [LIQUIDITY_POOL]);
    const liquidityBalance = lp ? lp.balance : "0";

    await SQLPromisePool.execute(`
        INSERT INTO metadata (key_name, key_value) VALUES 
        ('community_wallet_tokens', ?),
        ('liquidity_pool_tokens', ?)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)
    `, [communityBalance.toString(), liquidityBalance.toString()]);
}

async function fetchTransferLogs(fromBlock) {
    const maxBlockRange = 2000;
    const latestBlockHex = await getLatestBlockNumber();
    const latestBlock = parseInt(latestBlockHex, 16);
    let currentBlock = fromBlock;
    let allLogs = [];
    let logsProcessed = 0;
    while (currentBlock <= latestBlock) {
        const endBlock = Math.min(currentBlock + maxBlockRange - 1, latestBlock);
        const params = {
            jsonrpc: "2.0",
            id: 1,
            method: "eth_getLogs",
            params: [
                {
                    fromBlock: `0x${currentBlock.toString(16)}`,
                    toBlock: `0x${endBlock.toString(16)}`,
                    address: CONTRACT_ADDRESS
                },
            ],
        };
        const response = await axios.post(ALCHEMY_API_URL, params, { headers: { "Content-Type": "application/json" }});
        const logs = response.data.result || [];
        allLogs = allLogs.concat(logs);
        logsProcessed += logs.length;
        console.log(`Fetched logs from block ${currentBlock} to ${endBlock}. Logs count: ${logs.length}`);
        await updateLastProcessedBlock(endBlock);
        currentBlock = endBlock + 1;
    }
    console.log(`Total logs processed in fetch: ${logsProcessed}`);
    return allLogs;
}

async function processLogs(logs) {
    const insertTransactionQuery = `
        INSERT INTO transactions (tx_hash, block_number, from_address, to_address, amount, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    `;
    let logsProcessed = 0;
    const totalLogs = logs.length;

    for (const log of logs) {
        const txHash = log.transactionHash;
        const blockNumber = parseInt(log.blockNumber, 16);
        const blockTimestamp = await getBlockTimestamp(blockNumber);

        try {
            if (log.topics[0] === TRANSFER_TOPIC) {
                const fromAddress = `0x${log.topics[1].slice(26)}`;
                const toAddress = `0x${log.topics[2].slice(26)}`;
                const amount = BigInt(log.data);
                const amountDecimal = formatWeiToDecimal(amount);

                await SQLPromisePool.execute(insertTransactionQuery, [
                    txHash, blockNumber, fromAddress, toAddress, amountDecimal, blockTimestamp
                ]);

                if (toAddress.toLowerCase() === BURN_ADDRESS) {
                    if (fromAddress.toLowerCase() !== ZERO_ADDRESS && fromAddress.toLowerCase() !== BURN_ADDRESS) {
                        const [[exFrom]] = await SQLPromisePool.execute(`SELECT pledged_amount FROM pledgers WHERE address=?`,[fromAddress]);
                        let fromPledged=BigInt(0);if(exFrom) fromPledged=decimalToWeiBigInt(exFrom.pledged_amount.toString());
                        await upsertPledgerFinal(fromAddress, null, fromPledged, -amount, null, blockTimestamp, false);
                        const negativeAmountDecimal = `-${amountDecimal}`;
                        await updateHolderBalance(fromAddress, negativeAmountDecimal);
                    }
                    await increaseBurnedTokens(amountDecimal);
                } else if (fromAddress.toLowerCase() === ZERO_ADDRESS) {
                    if (toAddress.toLowerCase() !== ZERO_ADDRESS && toAddress.toLowerCase() !== BURN_ADDRESS) {
                        if (amount === AIRDROP_AMOUNT) {
                            // Airdrop scenario
                            const [[existing]] = await SQLPromisePool.execute(`SELECT pledged_amount,status FROM pledgers WHERE address=?`,[toAddress]);
                            let currentPledged=BigInt(0);let currentStatus=0;
                            if(existing){currentPledged=decimalToWeiBigInt(existing.pledged_amount.toString());currentStatus=existing.status;}
                            if(currentStatus!==2){
                              // Remove old pledge from total
                              if (currentPledged>0) await adjustTotalPledged(-currentPledged,blockTimestamp);
                              // Add new pledge amount
                              await adjustTotalPledged(AIRDROP_AMOUNT, blockTimestamp);
                              await upsertPledgerFinal(toAddress, 1, AIRDROP_AMOUNT, AIRDROP_AMOUNT, 1, blockTimestamp, true);
                            } else {
                              await upsertPledgerFinal(toAddress,null,currentPledged,amount,null,blockTimestamp,false);
                            }
                        } else {
                            // Another mint scenario
                            const [[exTo]] = await SQLPromisePool.execute(`SELECT pledged_amount FROM pledgers WHERE address=?`,[toAddress]);
                            let toPledged=BigInt(0);if(exTo) toPledged=decimalToWeiBigInt(exTo.pledged_amount.toString());
                            // no pledge changes
                            await upsertPledgerFinal(toAddress,null,toPledged,amount,null,blockTimestamp,false);
                        }
                        await updateHolderBalance(toAddress, amountDecimal);
                    }
                } else {
                    // Normal transfer
                    if (fromAddress.toLowerCase() !== ZERO_ADDRESS && fromAddress.toLowerCase() !== BURN_ADDRESS) {
                        const [[exFrom]] = await SQLPromisePool.execute(`SELECT pledged_amount FROM pledgers WHERE address=?`,[fromAddress]);
                        let fromPledged=BigInt(0);if(exFrom) fromPledged=decimalToWeiBigInt(exFrom.pledged_amount.toString());
                        await upsertPledgerFinal(fromAddress, null, fromPledged, -amount, null, blockTimestamp, false);
                        const negativeAmountDecimal = `-${amountDecimal}`;
                        await updateHolderBalance(fromAddress, negativeAmountDecimal);
                    }
                    if (toAddress.toLowerCase() !== ZERO_ADDRESS && toAddress.toLowerCase() !== BURN_ADDRESS) {
                        const [[exTo]] = await SQLPromisePool.execute(`SELECT pledged_amount FROM pledgers WHERE address=?`,[toAddress]);
                        let toPledged=BigInt(0);if(exTo) toPledged=decimalToWeiBigInt(exTo.pledged_amount.toString());
                        await upsertPledgerFinal(toAddress, null, toPledged, amount, null, blockTimestamp, false);
                        await updateHolderBalance(toAddress, amountDecimal);
                    }
                }

            } else if (log.topics[0] === CREATED_PLEDGE_TOPIC) {
                const pledgerAddress = `0x${log.topics[1].slice(26)}`;
                if (pledgerAddress.toLowerCase() !== ZERO_ADDRESS && pledgerAddress.toLowerCase() !== BURN_ADDRESS) {
                    const pledgerBalance = BigInt(log.data);
                    const [[existing]] = await SQLPromisePool.execute(`SELECT pledged_amount,status FROM pledgers WHERE address = ?`, [pledgerAddress]);
                    let currentPledged = BigInt(0);
                    let currentStatus = 0;
                    if (existing) {
                        currentPledged = decimalToWeiBigInt(existing.pledged_amount.toString());
                        currentStatus = existing.status;
                    }

                    if (currentStatus !== 2) {
                        // Remove old pledge
                        if (currentPledged>0) await adjustTotalPledged(-currentPledged, blockTimestamp);
                        // Add new pledge
                        await adjustTotalPledged(pledgerBalance, blockTimestamp);
                        await upsertPledgerFinal(pledgerAddress, null, pledgerBalance, BigInt(0), 1, blockTimestamp, true);
                    } else {
                        await upsertPledgerFinal(pledgerAddress, null, currentPledged, BigInt(0), null, blockTimestamp, false);
                    }
                }

            } else if (log.topics[0] === BROKEN_PLEDGE_TOPIC) {
                const pledgerAddress = `0x${log.topics[1].slice(26)}`;
                if (pledgerAddress.toLowerCase() !== ZERO_ADDRESS && pledgerAddress.toLowerCase() !== BURN_ADDRESS) {
                    const [[existing]] = await SQLPromisePool.execute(`SELECT pledged_amount FROM pledgers WHERE address = ?`, [pledgerAddress]);
                    if (existing) {
                        const pledgedAmount = decimalToWeiBigInt(existing.pledged_amount.toString());
                        if (pledgedAmount > 0) {
                            await adjustTotalPledged(-pledgedAmount, blockTimestamp);
                        }
                        // broken pledge: pledged_amount=0, status=2
                        // now also set broken_timestamp and broken_hash
                        await upsertPledgerFinal(pledgerAddress, null, BigInt(0), BigInt(0), 2, blockTimestamp, false, blockTimestamp, txHash);
                    }
                }
            }

            logsProcessed++;
            if (logsProcessed % 100 === 0 || logsProcessed === totalLogs) {
                const percentageProcessed = ((logsProcessed / totalLogs) * 100).toFixed(2);
                console.log(`Progress: ${percentageProcessed}% - Processed ${logsProcessed} out of ${totalLogs} logs`);
            }

        } catch (error) {
            console.error(`Error processing log for tx: ${txHash}`, error.message);
        }
    }

    await updateSpecialHolders();

    console.log(`Completed processing all ${totalLogs} logs.`);
}

async function main() {
    await createTables();
    const lastProcessedBlock = (await getLastProcessedBlock()) || CONTRACT_CREATION_BLOCK;
    console.log("Last processed block:", lastProcessedBlock);
    const logs = await fetchTransferLogs(lastProcessedBlock + 1);
    console.log(`Fetched ${logs.length} logs.`);
    if (logs.length > 0) {
        await processLogs(logs);
        const latestBlock = parseInt(logs[logs.length - 1].blockNumber, 16);
        await updateLastProcessedBlock(latestBlock);
        console.log(`Updated last processed block to ${latestBlock}`);
    } else {
        console.log("No new logs found.");
    }
}

setInterval(() => {
    main().catch((error) => console.error("Error in main loop:", error.message));
}, 300000);

main().catch((error) => console.error("Startup error:", error.message));
