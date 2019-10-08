package datahose

// Payload represents a datahose payload.
type Payload struct {
	Key  string                 `json:"key"`
	Body map[string]interface{} `json:"body"`
}
