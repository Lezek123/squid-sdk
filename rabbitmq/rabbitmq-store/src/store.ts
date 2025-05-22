import { ConfirmChannel } from 'amqplib'


type RabbitMQStoreConfig = {
    channel: ConfirmChannel
    blocksQueue: string
    onFirstSuccessfulPush?: () => Promise<void>
}

/**
 * A simplified interface for pushing blocks to RabbitMQ
 */
export class Store {
    public readonly isStream = true
    private pushedBlocks = 0
    constructor(private config: RabbitMQStoreConfig) {}

    pushBlock(block: { header: { height: number } }): Promise<void> {
        const { channel, blocksQueue, onFirstSuccessfulPush } = this.config
        return new Promise((resolve, reject) => {
            channel.sendToQueue(
                blocksQueue,
                Buffer.from(JSON.stringify(block)),
                {
                    persistent: true,
                    contentType: 'application/json',
                },
                (err, _ok) => {
                    if (err) {
                        return reject(err)
                    }
                    ++this.pushedBlocks
                    if (this.pushedBlocks == 1 && onFirstSuccessfulPush) {
                        return onFirstSuccessfulPush()
                            .then(resolve)
                            .catch(reject)
                    }
                    resolve()
                }
            )
        })
    }
}