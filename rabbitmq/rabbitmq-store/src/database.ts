import {SnakeNamingStrategy} from '@subsquid/typeorm-config/lib/namingStrategy'
import {createConnectionOptions} from '@subsquid/typeorm-config/lib/connectionOptions'
import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'
import {DataSource, EntityManager} from 'typeorm'
import {DatabaseState, FinalTxInfo, HashAndHeight} from './interfaces'
import {Store} from './store'
import { Logger, createLogger } from '@subsquid/logger'
import amqp from 'amqplib'

export type IsolationLevel = 'SERIALIZABLE' | 'READ COMMITTED' | 'REPEATABLE READ'

export interface RabbitMQDatabaseOptions {
    isolationLevel?: IsolationLevel
    stateSchema?: string
    queueName?: string
}


export class RabbitMQDatabase {
    public readonly supportsStreaming = true
    public readonly queue: string
    public readonly replyQueue: string = 'amq.rabbitmq.reply-to'
    private statusSchema: string
    private isolationLevel: IsolationLevel
    private postgresConn?: DataSource
    private amqpConn?: amqp.ChannelModel
    private _channel?: amqp.ConfirmChannel
    private log: Logger

    // TODO: Hardcoded to false now, as no support is available yet
    public readonly supportsHotBlocks = false as const

    constructor(options?: RabbitMQDatabaseOptions) {
        this.statusSchema = options?.stateSchema || 'squid_processor'
        this.isolationLevel = options?.isolationLevel || 'SERIALIZABLE'
        this.queue = options?.queueName || 'squid'
        this.log = createLogger('sqd:rabbitmq')
    }

    async connect(): Promise<DatabaseState> {
        assert(!this.postgresConn && !this.amqpConn, 'already connected')
        await this.initAMQP()
        const state = await this.initPostgres()
        return state
    }

    private get channel(): amqp.ConfirmChannel {
        assert(this._channel, 'cannot get channel: not connected')
        return this._channel
    }

    private async initAMQP() {
     const url = process.env.AMQP_URL || 'amqp://localhost'
        this.amqpConn = await amqp.connect(url)
        this._channel = await this.amqpConn.createConfirmChannel()
        this._channel.assertQueue(this.queue, {
            durable: true,
        })
    }

    private async initPostgres() {
        this.postgresConn = new DataSource({
            type: 'postgres',
            namingStrategy: new SnakeNamingStrategy(),
            ...createConnectionOptions()

        })

        await this.postgresConn.initialize()

        try {
            return await this.postgresConn.transaction('SERIALIZABLE', em => this.initTransaction(em))
        } catch(e: any) {
            await this.postgresConn.destroy().catch(() => {}) // ignore error
            this.postgresConn = undefined
            throw e
        }
    }

    async consumeRawMessages(handler: (content: string) => Promise<void>): Promise<void> {
        if (!this.amqpConn) {
            await this.initAMQP()
        }
        await this.channel.prefetch(1)
        this.channel.consume(this.queue, async (msg) => {
            if (msg) {
                await handler(msg.content.toString())
                this.channel.sendToQueue(msg.properties.replyTo, Buffer.from([1]), {
                    correlationId: msg.properties.correlationId
                })
                this.channel.ack(msg)
            }
        }, {
            noAck: false
        })
    }

    async disconnect(): Promise<void> {
        await this.postgresConn?.destroy().finally(() => this.postgresConn = undefined)
        await this.amqpConn?.close().finally(() => {
            this.amqpConn = undefined
            this._channel = undefined
        })
    }

    private async initTransaction(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        await em.query(
            `CREATE SCHEMA IF NOT EXISTS ${schema}`
        )
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.status (` +
            `id int4 primary key, ` +
            `height int4 not null, ` +
            `hash text DEFAULT '0x', ` +
            `nonce int4 DEFAULT 0`+
            `)`
        )
        await em.query( // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS hash text DEFAULT '0x'`
        )
        await em.query( // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS nonce int DEFAULT 0`
        )
        // TODO: ... hot blocks not supported yet

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )
        if (status.length == 0) {
            await em.query(`INSERT INTO ${schema}.status (id, height, hash) VALUES (0, -1, '0x')`)
            status.push({height: -1, hash: '0x', nonce: 0})
        }

        let top: HashAndHeight[] = [] // TODO: Not supported yet

        return assertStateInvariants({...status[0], top})
    }

    private async getState(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT height, hash, nonce FROM ${schema}.status WHERE id = 0`
        )

        assert(status.length == 1)

        let top: HashAndHeight[] = [] // TODO: Not supported yet

        return assertStateInvariants({...status[0], top})
    }

    private async blockConsumed(height: number): Promise<void> {
        const { replyQueue, channel } = this
        return new Promise(async (resolve, reject) => {
            try {
                const { consumerTag } = await channel.consume(replyQueue, (msg) => {
                    if (!msg) {
                        return
                    }
                    const msgBlockHeight: string = msg.properties.correlationId || '?'
                    if (!msg.content.equals(Buffer.from([1]))) {
                        this.log.warn(`Recieved error from consumer on block ${msgBlockHeight}: ${msg.content.toString()}`)
                    }
                    else if (msgBlockHeight === height.toString()) {
                        channel.cancel(consumerTag).then(() => resolve()).catch((e) => reject(e))
                    }
                }, {
                    noAck: true
                })
            } catch(e) {
                reject(e)
            }
        })

    }

    transact(info: FinalTxInfo, cb: (store: Store) => Promise<void>): Promise<void> {
        return this.submit(async em => {
            const state = await this.getState(em)
            const { prevHead: prev, nextHead: next } = info

            assert(state.hash === info.prevHead.hash, RACE_MSG)
            assert(state.height === prev.height)
            assert(prev.height < next.height)
            assert(prev.hash != next.hash)

            const { channel, queue, replyQueue } = this
            
            const queueConsumed = this.blockConsumed(next.height)
            await cb(new Store({ channel, queue, replyQueue }))
            await queueConsumed
            

            await this.updateStatus(em, state.nonce, next)
        })
    }

    private async updateStatus(em: EntityManager, nonce: number, next: HashAndHeight): Promise<void> {
        let schema = this.escapedSchema()

        let result: [data: any[], rowsChanged: number] = await em.query(
            `UPDATE ${schema}.status SET height = $1, hash = $2, nonce = nonce + 1 WHERE id = 0 AND nonce = $3`,
            [next.height, next.hash, nonce]
        )

        let rowsChanged = result[1]

        // Will never happen if isolation level is SERIALIZABLE or REPEATABLE_READ,
        // but occasionally people use multiprocessor setups and READ_COMMITTED.
        assert.strictEqual(
            rowsChanged,
            1,
            RACE_MSG
        )
    }

    private async submit(tx: (em: EntityManager) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                const conn = this.postgresConn
                assert(conn != null, 'not connected')
                return await conn.transaction(this.isolationLevel, tx)
            } catch(e: any) {
                if (e.code == '40001' && retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    private escapedSchema(): string {
        const conn = assertNotNull(this.postgresConn)
        return conn.driver.escape(this.statusSchema)
    }
}


const RACE_MSG = 'status table was updated by foreign process, make sure no other processor is running'


function assertStateInvariants(state: DatabaseState): DatabaseState {
    let height = state.height

    // Sanity check. Who knows what driver will return?
    assert(Number.isSafeInteger(height))

    assertChainContinuity(state, state.top)

    return state
}


function assertChainContinuity(base: HashAndHeight, chain: HashAndHeight[]) {
    let prev = base
    for (let b of chain) {
        assert(b.height === prev.height + 1, 'blocks must form a continues chain')
        prev = b
    }
}
