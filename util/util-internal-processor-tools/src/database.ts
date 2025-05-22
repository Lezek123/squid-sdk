/**
 * Database is responsible for providing a persistent storage for data handlers
 * and keeping the processor progress and status.
 */
export type Database<S> = FinalDatabase<S> | HotDatabase<S>
export type DatabaseWithStream<S> = FinalDatabase<S> & {
    supportsStreaming: true
    consumeRawMessages: (handler: (block: string) => Promise<void>) => Promise<void>
}
export type DatabaseWithoutStream<S> = Database<S> & { supportsStreaming?: false }

export interface StreamStore {
    pushBlock(block: { header: { height: number } }, onConfirmed?: () => void, onError?: (err: any) => void): void
}


export interface FinalTxInfo {
    prevHead: HashAndHeight
    nextHead: HashAndHeight
    isOnTop: boolean
}


export interface FinalDatabase<S> {
    supportsHotBlocks?: false
    connect(): Promise<HashAndHeight>
    transact(info: FinalTxInfo, cb: (store: S) => Promise<void>): Promise<void>
}


export interface HotTxInfo {
    finalizedHead: HashAndHeight
    baseHead: HashAndHeight
    newBlocks: HashAndHeight[]
}


export interface HotDatabase<S> {
    supportsHotBlocks: true
    connect(): Promise<HotDatabaseState>
    transact(info: FinalTxInfo, cb: (store: S) => Promise<void>): Promise<void>
    /**
     * @deprecated
     */
    transactHot(info: HotTxInfo, cb: (store: S, block: HashAndHeight) => Promise<void>): Promise<void>

    transactHot2?(
        info: HotTxInfo,
        cb: (store: S, blockSliceStart: number, blockSliceEnd: number) => Promise<void>
    ): Promise<void>
}


export interface HotDatabaseState extends HashAndHeight {
    top: HashAndHeight[]
}


export interface HashAndHeight {
    height: number
    hash: string
}
