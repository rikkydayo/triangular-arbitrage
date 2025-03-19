package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var (
	apiKey          string
	apiSecret       string
	slackWebhookURL string
	baseURL         = "https://testnet.binance.vision/api/v3"
	wsURL           = "wss://testnet.binance.vision/ws"
)

func init() {
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println("環境変数の読み込みに失敗:", err)
		return
	}
	apiKey = os.Getenv("BINANCE_API_KEY")
	apiSecret = os.Getenv("BINANCE_API_SECRET")
	slackWebhookURL = os.Getenv("SLACK_WEBHOOK_URL")
	if apiKey == "" || apiSecret == "" || slackWebhookURL == "" {
		panic("APIキーまたはSlack Webhook URLが設定されていません")
	}
}

type Signal struct {
	Prices     map[string]float64 `json:"prices"`
	ProfitRate float64            `json:"profit_rate"`
	Direction  string             `json:"direction"`
}

type OrderResponse struct {
	OrderID  int64   `json:"orderId"`
	Status   string  `json:"status"`
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"`
	Quantity float64 `json:"executedQty,string"`
	Price    float64 `json:"price,string"`
}

func sign(query string) string {
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(query))
	return hex.EncodeToString(h.Sum(nil))
}

func placeOrder(pair, side string, quantity float64) (*OrderResponse, error) {
	params := url.Values{}
	params.Add("symbol", pair)
	params.Add("side", side)
	params.Add("type", "MARKET")
	params.Add("quantity", fmt.Sprintf("%.8f", quantity))
	params.Add("timestamp", fmt.Sprintf("%d", time.Now().UnixMilli()))
	query := params.Encode()
	params.Add("signature", sign(query))

	req, _ := http.NewRequest("POST", baseURL+"/order", nil)
	req.URL.RawQuery = params.Encode()
	req.Header.Set("X-MBX-APIKEY", apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var order OrderResponse
	json.NewDecoder(resp.Body).Decode(&order)
	return &order, nil
}

func cancelOrder(orderID int64, pair string) error {
	params := url.Values{}
	params.Add("symbol", pair)
	params.Add("orderId", fmt.Sprintf("%d", orderID))
	params.Add("timestamp", fmt.Sprintf("%d", time.Now().UnixMilli()))
	query := params.Encode()
	params.Add("signature", sign(query))

	req, _ := http.NewRequest("DELETE", baseURL+"/order", nil)
	req.URL.RawQuery = params.Encode()
	req.Header.Set("X-MBX-APIKEY", apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func rollback(orders []*OrderResponse) {
	for _, order := range orders {
		if order == nil {
			continue
		}
		if order.Status != "FILLED" {
			err := cancelOrder(order.OrderID, order.Symbol)
			if err != nil {
				fmt.Println("キャンセル失敗:", err)
				sendSlackNotification("ロールバック失敗: キャンセルエラー")
			}
		} else if order.Side == "BUY" && order.Quantity > 0 {
			side := "SELL"
			if order.Symbol == "ETHUSDT" {
				side = "BUY" // ETH/USDTは逆
			}
			retry := 3
			success := false
			for i := 0; i < retry; i++ {
				_, err := placeOrder(order.Symbol, side, order.Quantity)
				if err == nil {
					success = true
					break
				}
				fmt.Println("反対売買失敗:", err)
				time.Sleep(1 * time.Second)
			}
			if !success {
				sendSlackNotification("ロールバック失敗: 反対売買エラー")
			}
		}
	}
}

func sendSlackNotification(message string) {
	payload := map[string]string{"text": message}
	data, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", slackWebhookURL, nil)
	req.Header.Set("Content-Type", "application/json")
	req.URL.RawQuery = url.Values{"payload": {string(data)}}.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Slack通知失敗:", err)
		return
	}
	resp.Body.Close()
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	ws, _, err := websocket.DefaultDialer.Dial(wsURL+"/btcusdt@bookTicker", nil)
	if err != nil {
		fmt.Println("WebSocketエラー:", err)
		return
	}
	defer ws.Close()

	go func() {
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				fmt.Println("WebSocket読み込みエラー:", err)
				return
			}
			fmt.Println("リアルタイム価格:", string(message))
		}
	}()

	for {
		signalStr, err := rdb.Get(ctx, "arbitrage_signal").Result()
		if err == redis.Nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			fmt.Println("Redisエラー:", err)
			continue
		}

		var signal Signal
		json.Unmarshal([]byte(signalStr), &signal)
		prices := signal.Prices

		if signal.ProfitRate > 0.1 {
			var wg sync.WaitGroup
			success := make(chan bool, 3)
			orders := make([]*OrderResponse, 3)
			wg.Add(3)

			if signal.Direction == "順方向" {
				go func() {
					defer wg.Done()
					order, err := placeOrder("BTCUSDT", "BUY", 100000/prices["BTC/USDT"])
					orders[0] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
				go func() {
					defer wg.Done()
					order, err := placeOrder("ETHBTC", "BUY", (100000/prices["BTC/USDT"])/prices["ETH/BTC"])
					orders[1] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
				go func() {
					defer wg.Done()
					order, err := placeOrder("ETHUSDT", "SELL", (100000/prices["BTC/USDT"])/prices["ETH/BTC"])
					orders[2] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
			} else {
				go func() {
					defer wg.Done()
					order, err := placeOrder("ETHUSDT", "BUY", 100000/prices["ETH/USDT"])
					orders[0] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
				go func() {
					defer wg.Done()
					order, err := placeOrder("ETHBTC", "SELL", (100000/prices["ETH/USDT"])*prices["ETH/BTC"])
					orders[1] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
				go func() {
					defer wg.Done()
					order, err := placeOrder("BTCUSDT", "SELL", (100000/prices["ETH/USDT"])*prices["ETH/BTC"])
					orders[2] = order
					success <- (err == nil && order.Status == "FILLED")
				}()
			}

			timeout := time.After(500 * time.Millisecond)
			done := make(chan bool)
			go func() { wg.Wait(); done <- true }()

			allSuccess := true
			for i := 0; i < 3; i++ {
				select {
				case ok := <-success:
					if !ok {
						allSuccess = false
					}
				case <-timeout:
					fmt.Println("タイムアウト、取引キャンセル")
					allSuccess = false
				}
			}

			if allSuccess {
				message := fmt.Sprintf("取引成功！利益率: %.2f%%", signal.ProfitRate)
				fmt.Println(message)
				sendSlackNotification(message)
			} else {
				fmt.Println("取引失敗、ロールバック開始")
				rollback(orders)
			}
		} else {
			fmt.Println("利益率が低い:", signal.ProfitRate)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
