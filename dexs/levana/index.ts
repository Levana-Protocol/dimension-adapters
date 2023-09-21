import { Adapter, ChainBlocks, IStartTimestamp } from "../../adapters/types";
import { CHAIN } from "../../helpers/chains";
import fetchURL from "../../utils/fetchURL";
import BigNumber from "bignumber.js";
import {
  getTimestampAtStartOfDayUTC,
  getTimestampAtStartOfPreviousDayUTC,
} from "../../utils/date";

const indexer = "https://indexer.levana.finance";

interface Market {
  chain: string;
  contract: string;
}

interface TotalFees {
  name: string;
  total: BigNumber;
  total_usd: BigNumber;
}

interface TotalRevenue {
  total: BigNumber;
  total_usd: BigNumber;
}

type TradeVolume = Record<string, string>;
type TradeVolumeResp = Record<string, TradeVolume>;

// Volume

const getTotalVolume = async (timestamp: number, markets: string[]) => {
  const prevDayTimestamp = getTimestampAtStartOfPreviousDayUTC(timestamp);
  const prevDayDate = dateStr(prevDayTimestamp);
  const url = `${indexer}/cumulative-trade-volume?scope=daily&start_date=${prevDayDate}&end_date=${prevDayDate}`;
  const resp: TradeVolumeResp = (await fetchURL(url)).data;

  let totalVolume: BigNumber = BigNumber(0);
  let volumes = resp[prevDayDate];

  if (volumes === undefined) {
    throw Error(`unable to retrieve daily volume for ${prevDayDate}`);
  }

  for (const market in volumes) {
    if (markets.includes(market)) {
      totalVolume = totalVolume.plus(BigNumber(volumes[market]));
    }
  }

  return totalVolume.toString();
};

const getDailyVolume = async (timestamp: number, markets: string[]) => {
  const startTimestamp = getTimestampAtStartOfPreviousDayUTC(timestamp);
  const endTimestamp = getTimestampAtStartOfDayUTC(timestamp);
  const startDate = dateStr(startTimestamp);
  const endDate = dateStr(endTimestamp);
  const url = `${indexer}/trade-volume?scope=daily&start_date=${startDate}&end_date=${endDate}`;
  const resp: TradeVolumeResp = (await fetchURL(url)).data;

  let totalVolume: BigNumber = BigNumber(0);
  let volumes = resp[startDate];

  if (volumes === undefined) {
    throw Error(`unable to retrieve daily volume for ${startDate}`);
  }

  for (const market in volumes) {
    if (markets.includes(market)) {
      totalVolume = totalVolume.plus(BigNumber(volumes[market]));
    }
  }

  return totalVolume.toString();
};

// Fees

const getFees = async (
  markets: string[],
  endTimestamp: number,
  startTimestamp?: number
) => {
  let qStr =
    "market=" + markets.join("&market=") + `&end_timestamp=${endTimestamp}`;

  if (startTimestamp !== undefined) {
    qStr += `&start_timestamp=${startTimestamp}`;
  }

  const url = `${indexer}/get-total-fees?${qStr}`;
  const resp: TotalFees[] = (await fetchURL(url)).data;

  let total = resp.reduce(
    (result, current) => result.plus(current.total_usd),
    BigNumber(0)
  );

  return total.toString();
};

const getTotalFees = async (timestamp: number, markets: string[]) => {
  const endTimestamp = getTimestampAtStartOfDayUTC(timestamp);
  const totalFees = await getFees(markets, endTimestamp);

  return totalFees;
};

const getDailyFees = async (timestamp: number, markets: string[]) => {
  const startTimestamp = getTimestampAtStartOfPreviousDayUTC(timestamp);
  const endTimestamp = getTimestampAtStartOfDayUTC(timestamp);
  const totalFees = await getFees(markets, endTimestamp, startTimestamp);

  return totalFees;
};

// Revenue

const getRevenue = async (
  markets: string[],
  path: string,
  endTimestamp: number,
  startTimestamp?: number
) => {
  let qStr =
    "?market=" + markets.join("&market=") + `&end_timestamp=${endTimestamp}`;

  if (startTimestamp !== undefined) {
    qStr += `&start_timestamp=${startTimestamp}`;
  }

  const url = indexer + path + qStr;
  const resp: TotalRevenue = (await fetchURL(url)).data;

  return resp.total_usd.toString();
};

const getTotalRevenue = async (
  timestamp: number,
  markets: string[],
  path: string
) => {
  const endTimestamp = getTimestampAtStartOfDayUTC(timestamp);
  const totalRevenue = await getRevenue(markets, path, endTimestamp);

  return totalRevenue;
};

const getDailyRevenue = async (
  timestamp: number,
  markets: string[],
  path: string
) => {
  const startTimestamp = getTimestampAtStartOfPreviousDayUTC(timestamp);
  const endTimestamp = getTimestampAtStartOfDayUTC(timestamp);
  const totalRevenue = await getRevenue(
    markets,
    path,
    endTimestamp,
    startTimestamp
  );

  return totalRevenue;
};

// Helpers

const getMarketAddrs = async (chainId: string) => {
  const url = `${indexer}/markets`;
  const markets: [Market] = (await fetchURL(url))?.data;

  return markets
    .filter((market) => chainId === market.chain)
    .map((market) => market.contract);
};

const dateStr = (timestamp: number) => {
  let date = new Date(timestamp * 1000);
  return `${date.getUTCFullYear()}-${
    date.getUTCMonth() + 1
  }-${date.getUTCDate()}`;
};

// Fetch

const fetch = async (timestamp: number, chainId: string) => {
  const marketAddrs = await getMarketAddrs(chainId);
  const lpProfitPath = "/get-lp-profits";
  const protocolProfitPath = "/get-protocol-profits";

  const dimensionRequests = [
    getDailyVolume(timestamp, marketAddrs),
    getDailyFees(timestamp, marketAddrs),
    getDailyRevenue(timestamp, marketAddrs, protocolProfitPath),
    getDailyRevenue(timestamp, marketAddrs, lpProfitPath),
    getTotalVolume(timestamp, marketAddrs),
    getTotalFees(timestamp, marketAddrs),
    getTotalRevenue(timestamp, marketAddrs, protocolProfitPath),
    getTotalRevenue(timestamp, marketAddrs, lpProfitPath),
  ];
  const [
    dailyVolume,
    dailyUserFees,
    dailyProtocolRevenue,
    dailySupplySideRevenue,
    totalVolume,
    totalFees,
    totalProtocolRevenue,
    totalSupplySideRevenue,
  ] = await Promise.all(dimensionRequests);

  return {
    timestamp,
    dailyVolume,
    dailyFees: dailyUserFees,
    dailyUserFees,
    dailyRevenue: dailyProtocolRevenue,
    dailyProtocolRevenue,
    dailySupplySideRevenue,
    totalVolume,
    totalFees,
    totalUserFees: totalFees,
    totalRevenue: totalProtocolRevenue,
    totalProtocolRevenue,
    totalSupplySideRevenue,
  };
};

interface ChainConfig {
  chainId: string;
  start: number;
}

// The start timestamps refer to the launch of the ATOM/USD market and SEI/USD market respectively,
// which were the first markets on their chains.
const config: Record<string, ChainConfig> = {
  [CHAIN.OSMOSIS]: { chainId: "osmosis-1", start: 1686025556 },
  [CHAIN.SEI]: { chainId: "pacific-1", start: 1692345706 },
};

const adapter: Adapter = {
  adapter: {},
};

Object.keys(config).forEach((chain) => {
  adapter.adapter[chain] = {
    start: async () => config[chain].start,
    fetch: async (timestamp) => fetch(timestamp, config[chain].chainId),
    runAtCurrTime: true,
    meta: {
      methodology: {
        Fees: "fees methodology",
        Revenue: "revenue methodology",
        SupplySideRevenue: "supply side revenue methodology",
      },
    },
  };
});

export default adapter;
