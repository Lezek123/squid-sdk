import { ConfirmChannel } from 'amqplib'


type RabbitMQStoreConfig = {
    channel: ConfirmChannel
    queue: string
    replyQueue: string
}

/**
 * A simplified interface for pushing blocks to RabbitMQ
 */
export class Store {
    public readonly isStream = true
    constructor(private config: RabbitMQStoreConfig) {}

    pushBlock(block: {
        header: { height: number }
    }, onConfirmed?: () => void, onError?: (err: any) => void) {
        const { channel, queue, replyQueue } = this.config
        channel.sendToQueue(queue, Buffer.from(JSON.stringify(block)), {
            persistent: true,
            contentType: 'application/json',
            replyTo: replyQueue,
            // TODO: Only possible because we don't process hot blocks!
            correlationId: block.header.height.toString(),
        }, (err, _ok) => {
            if (err) {
                return onError?.(err)
            }
            onConfirmed?.()
        })
    }
}