package main

type TxnState string

const (
	StateInit      TxnState = "INIT"
	StateCommitted TxnState = "COMMITTED"
	StateAborted   TxnState = "ABORTED"
)

// TransactionMetadata: Request from Client
type TransactionMetadata struct {
	ID           string   `json:"id"`
	Keys         []string `json:"keys"`
	Participants []string `json:"participants"`
}

// RPC Requests
type PrepareRequest struct {
	TxnID string   `json:"txn_id"`
	Keys  []string `json:"keys"`
}

type CommitRequest struct {
	TxnID  string `json:"txn_id"`
	Action string `json:"action"` // "COMMIT" or "ABORT"
}