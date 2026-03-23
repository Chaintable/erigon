package types

import "github.com/erigontech/erigon-lib/common"

type BlockLoad struct {
	Hash         common.Hash
	AccountLoads []common.Address
	StorageLoads []AccountStorageLoad
}

type AccountStorageLoad struct {
	Address common.Address
	Keys    []common.Hash
}
