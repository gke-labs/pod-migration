package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"

	"google3/third_party/golang/gorilla/websocket/websocket"
)

const (
	token = "pm-test-token"
	host  = "127.0.0.1:8888"
)

type KernelResponse struct {
	ID string `json:"id"`
}

type MsgHeader struct {
	MsgID   string `json:"msg_id"`
	MsgType string `json:"msg_type"`
	Session string `json:"session"`
	Version string `json:"version"`
}

type Msg struct {
	Header       MsgHeader              `json:"header"`
	ParentHeader map[string]interface{} `json:"parent_header"`
	Metadata     map[string]interface{} `json:"metadata"`
	Content      map[string]interface{} `json:"content"`
	Channel      string                 `json:"channel"`
}

func main() {
	log.Println("[*] Starting WebSocket client...")

	// 1. Create a new kernel via REST API
	kernelID, err := createKernel()
	if err != nil {
		log.Fatalf("[-] Failed to create kernel: %v", err)
	}
	log.Printf("[+] Created kernel: %s", kernelID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// 2. Establish and maintain WebSocket connection
	u := url.URL{Scheme: "ws", Host: host, Path: fmt.Sprintf("/api/kernels/%s/channels", kernelID), RawQuery: "token=" + token}
	log.Printf("[*] Connecting to WebSocket: %s", u.String())

	// Start execution of Python print loop
	err = sendExecutionRequest(kernelID)
	if err != nil {
		log.Fatalf("[-] Failed to send execution request: %v", err)
	}

	for {
		err := runWebSocket(u.String(), interrupt)
		if err != nil {
			log.Printf("[!] WebSocket disconnected: %v. Retrying in 1 second...", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
}

func createKernel() (string, error) {
	apiURL := fmt.Sprintf("http://%s/api/kernels?token=%s", host, token)
	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer([]byte("{}")))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var kr KernelResponse
	err = json.NewDecoder(resp.Body).Decode(&kr)
	if err != nil {
		return "", err
	}
	return kr.ID, nil
}

func sendExecutionRequest(kernelID string) error {
	// We need to send the execution request over HTTP or WS?
	// The Jupyter API allows sending execute_request on the shell channel of the websocket connection.
	// But to make it simple, we will send it inside our runWebSocket loop once the websocket is open!
	return nil
}

func runWebSocket(wsURL string, interrupt chan os.Signal) error {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 3 * time.Second,
	}
	c, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("dial failed with status %d: %s (%w)", resp.StatusCode, string(body), err)
		}
		return err
	}
	defer c.Close()
	log.Println("[+] WebSocket connected successfully!")

	// Send execution request on connect
	msg := Msg{
		Header: MsgHeader{
			MsgID:   "exec_1",
			MsgType: "execute_request",
			Session: "session_1",
			Version: "5.3",
		},
		ParentHeader: make(map[string]interface{}),
		Metadata:     make(map[string]interface{}),
		Content: map[string]interface{}{
			"code":             "import time\ni = 0\nwhile True:\n    i += 1\n    print(f'WS Count: {i}', flush=True)\n    time.sleep(0.5)\n",
			"silent":           false,
			"store_history":    true,
			"user_expressions": make(map[string]interface{}),
			"allow_stdin":      false,
			"stop_on_error":    true,
		},
		Channel: "shell",
	}

	err = c.WriteJSON(msg)
	if err != nil {
		return fmt.Errorf("failed to write execution request: %w", err)
	}
	log.Println("[*] Execution request sent.")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("[!] Read error: %v", err)
				return
			}
			
			var incoming Msg
			err = json.Unmarshal(message, &incoming)
			if err != nil {
				continue
			}

			if incoming.Header.MsgType == "stream" {
				if text, ok := incoming.Content["text"]; ok {
					fmt.Print(text)
				}
			}
		}
	}()

	for {
		select {
		case <-done:
			return fmt.Errorf("read loop ended")
		case <-interrupt:
			log.Println("[*] Interrupt received, closing connection...")
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("[-] Write close error:", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			os.Exit(0)
		}
	}
}
