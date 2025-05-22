import { RabbitMQDatabase } from '@subsquid/rabbitmq-store'
import { processor } from './processor'
import { balances } from './types/events'
import { createLogger } from '@subsquid/logger'

const logger = createLogger('consumer')

let consumedBlocks = 0
let startBlock = 0
let lastProcessedBlock = 0
let totalTransferredAmount = 0n

setInterval(() => {
        logger.info(
            `Consumed blocks: ${consumedBlocks} ` +
            `([${startBlock}, ${lastProcessedBlock}]), ` +
            `total amount transferred: ${totalTransferredAmount}`)
}, 5000);

processor.comsume(new RabbitMQDatabase(), async (block) => {
    // Note: Consumer still needs to use some kind of de-duplication logic to avoid
    // processing the same message twice. If the publisher crashes, it will
    // start streaming from the last block that ended a successfully processed batch.
    for (const event of block.events) {
        if (balances.transfer.v1020.is(event)) {
            const [_from, _to, value, _fees] = balances.transfer.v1020.decode(event)
            totalTransferredAmount += value
        }
        if (balances.transfer.v1050.is(event)) {
            const [_from, _to, value] = balances.transfer.v1050.decode(event)
            totalTransferredAmount += value
        }
        if (balances.transfer.v9130.is(event)) {
            const { amount } = balances.transfer.v9130.decode(event)
            totalTransferredAmount += amount
        }
    }
    startBlock = startBlock || block.header.height
    lastProcessedBlock = block.header.height
    ++consumedBlocks
})