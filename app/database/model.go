package database

type Transaction struct {
	AccountID       string  `json:"account_id"`
	TransactionType string  `json:"transaction_type,omitempty"`
	Amount          float64 `json:"amount"`
}

type Account struct {
	AccountID string  `json:"account_id"`
	Balance   float64 `json:"balance"`
}
