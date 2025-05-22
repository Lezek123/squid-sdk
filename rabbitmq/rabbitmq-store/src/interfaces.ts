export interface HashAndHeight {
    height: number
    hash: string
}


export interface ConsumerState extends HashAndHeight {
    // nonce: number
}


export interface FinalTxInfo {
    prevHead: HashAndHeight
    nextHead: HashAndHeight
}