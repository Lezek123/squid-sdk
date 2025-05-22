import {SubstrateBatchProcessor} from '@subsquid/substrate-processor'
import {events} from './types'


export const processor = new SubstrateBatchProcessor()
    .setGateway('https://v2.archive.subsquid.io/network/kusama')
    .setRpcEndpoint(process.env.KUSAMA_NODE_WS || 'wss://kusama-rpc.polkadot.io')
    .setFields({
        block: {
            timestamp: true
        }
    })
    .setBlockRange({from: 19_666_100})
    .addEvent({
        name: [events.balances.transfer.name]
    })