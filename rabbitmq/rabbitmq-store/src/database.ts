import assert from 'assert'
import {ConsumerState, FinalTxInfo} from './interfaces'
import {Store} from './store'
import { Logger, createLogger } from '@subsquid/logger'
import amqp from 'amqplib'
import { randomUUID } from 'crypto'

export interface RabbitMQDatabaseOptions {
    blocksQueueName?: string
    consumerStateQueueName?: string
}


export class RabbitMQDatabase {
    public readonly supportsStreaming = true
    public readonly blocksQueue: string
    public readonly consumerStateQueue: string
    public readonly replyQueue: string = 'amq.rabbitmq.reply-to'
    private conn?: amqp.ChannelModel
    private _channel?: amqp.ConfirmChannel
    private log: Logger
    private cachedBlockQueueSize: number = 0
    private lastConsumerStateMsg?: amqp.ConsumeMessage

    // TODO: Hardcoded to false now, as no support is available yet
    public readonly supportsHotBlocks = false as const

    constructor(options?: RabbitMQDatabaseOptions) {
        this.blocksQueue = options?.blocksQueueName || 'sqd.blocks'
        this.consumerStateQueue = options?.consumerStateQueueName || 'sqd.consumer-state'
        this.log = createLogger('sqd:rabbitmq')
    }

    async connect(): Promise<ConsumerState> {
        const state = await this.initPublisher()
        return state
    }

    private get channel(): amqp.ConfirmChannel {
        assert(this._channel, 'cannot get channel: not connected')
        return this._channel
    }

    private async emptyBlockQueue(): Promise<void> {
        this.log.debug('Waiting for block queue to be empty...')
        while (true) {
            const { messageCount: blocksQueueSize } = await this.channel.checkQueue(this.blocksQueue)
            if (blocksQueueSize === 0) {
                this.log.debug('Block queue is empty.')
                return
            }
            await new Promise((resolve) => setInterval(resolve, 1000))
        }
    }

    private async initPublisher(): Promise<ConsumerState> {
        await this.initAmqp()
        return new Promise(async (resolve, reject) => {
            try {
                await this.emptyBlockQueue()
                // Consume state updates from client
                this.log.debug(`Waiting for consumer state...`)
                const consumerTag = randomUUID()
                await this.channel.consume(
                    this.consumerStateQueue,
                    async (msg) => {
                        if (msg) {
                            let state: ConsumerState
                            try {
                                state = JSON.parse(msg.content.toString())
                                assertStateInvariants(state)
                            } catch(e) {
                                return reject(`Invalid consumer state message: ${e instanceof Error ? e.message : e}`)
                            }
                            this.lastConsumerStateMsg = msg
                            this.log.debug(`Retrieved consumer state: ${JSON.stringify(state)}`)
                            this.channel.cancel(consumerTag)
                            resolve(state)
                        }
                    },
                    {
                        // Will ack once we successfully send first block to the blocks queue
                        noAck: false,
                        consumerTag
                    }
                )
            } catch(e) {
                reject(e)
            }
        })
    }

    private async initAmqp(): Promise<{
        blocksQueueState: amqp.Replies.AssertQueue,
        consumerQueueState: amqp.Replies.AssertQueue
    }> {
        assert(!this.conn, 'already connected')
        const url = process.env.AMQP_URL || 'amqp://localhost'
        this.conn = await amqp.connect(url)
        this._channel = await this.conn.createConfirmChannel()
        const blocksQueueState = await this.channel.assertQueue(this.blocksQueue, {
            durable: true,
            // TODO: single active customer
            // (https://www.rabbitmq.com/docs/consumers#enabling-single-active-consumer-on-quorum-and-classic-queues)
        })
        const consumerQueueState = await this.channel.assertQueue(this.consumerStateQueue, {
            durable: true,
            maxLength: 1, // Drop old state updates
        })
        return { blocksQueueState, consumerQueueState }
    }

    private sendMessage(queue: string, message: object, options?: amqp.Options.Publish): Promise<void> {
        return new Promise((resolve, reject) => {
            this.channel.sendToQueue(
                queue,
                Buffer.from(JSON.stringify(message)),
                {
                    ...(options || {}),
                    persistent: true,
                    contentType: 'application/json',
                },
                (err, ok) => { err ? reject(err) : resolve() }
            )
        })
    }

    private updateConsumerState(state: ConsumerState): Promise<void> {
        this.log.debug(`Updating consumer state: ${JSON.stringify(state)}`)
        return this.sendMessage(this.consumerStateQueue, state)
    }

    private async initConsumer(): Promise<void> {
        const { blocksQueueState: { messageCount } } = await this.initAmqp()
        this.cachedBlockQueueSize = messageCount
        if (messageCount === 0) {
            // Request the first batch of blocks...
            await this.updateConsumerState({ height: -1, hash: '0x' })
        }
    }

    private async checkBlockQueueSize(): Promise<number> {
        if (this.cachedBlockQueueSize <= 0) {
            const { messageCount } = await this.channel.checkQueue(this.blocksQueue)
            this.cachedBlockQueueSize = messageCount
            return messageCount
        }
        // The assumption is that there is only 1 consumer,
        // so the messages won't disappear from the queue without
        // the consumer process being aware of it
        return this.cachedBlockQueueSize
    }

    async consumeRawMessages(handler: (content: string) => Promise<ConsumerState>): Promise<void> {
        if (!this.conn) {
            await this.initConsumer()
        }
        await this.channel.prefetch(1)
        this.channel.consume(this.blocksQueue, async (msg) => {
            --this.cachedBlockQueueSize
            if (msg) {
                const newState = await handler(msg.content.toString())
                const queueSize = await this.checkBlockQueueSize()
                if (queueSize === 0) {
                    await this.updateConsumerState(newState)
                }
                this.channel.ack(msg)
            }
        }, {
            noAck: false
        })
    }

    async disconnect(): Promise<void> {
        await this.conn?.close().finally(() => {
            this.conn = undefined
            this._channel = undefined
        })
    }

    async transact(_info: FinalTxInfo, cb: (store: Store) => Promise<void>): Promise<void> {
        // const { prevHead: prev, nextHead: next } = info
        // assert(state.hash === info.prevHead.hash, RACE_MSG)
        // assert(state.height === prev.height)
        // assert(prev.height < next.height)
        // assert(prev.hash != next.hash)
        const { channel, blocksQueue } = this
        await cb(new Store({ channel, blocksQueue, onFirstSuccessfulPush: async () => {
            if (this.lastConsumerStateMsg) {
                this.channel.ack(this.lastConsumerStateMsg)
                this.lastConsumerStateMsg = undefined
            }
        } }))
    }
}

function assertStateInvariants(state: ConsumerState): ConsumerState {
    // Sanity checks
    assert(state.height && typeof state.height === 'number' && Number.isSafeInteger(state.height))
    assert(state.hash && typeof state.hash === 'string')
    return state
}
