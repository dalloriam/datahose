package datahose

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// Client represents a datahose client.
type Client struct {
	cfg config
}

// NewClient returns a datahose client
func NewClient() (*Client, error) {
	cfg, err := getConfig()
	if err != nil {
		return nil, err
	}

	return &Client{cfg}, nil
}

// Push pushes an event to the datahose.
func (c *Client) Push(payload Payload) error {
	outBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.cfg.ServiceHost, bytes.NewBuffer(outBytes))
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", fmt.Sprintf("UPW %s %s", c.cfg.Email, c.cfg.Password)) // TODO: Support using Firebase auth directly instead of delegating auth to the hose.
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}
