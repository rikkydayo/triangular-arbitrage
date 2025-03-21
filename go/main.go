package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// NotifyRequest は Python から送られてくるリクエストの構造体
type NotifyRequest struct {
	Triangle   string  `json:"triangle"`
	Direction  string  `json:"direction"`
	ProfitRate float64 `json:"profit_rate"`
	ProfitUSDT float64 `json:"profit_usdt"`
	Volatility float64 `json:"volatility"`
	Slippage   float64 `json:"slippage"`
	Trend      string  `json:"trend"`
	Threshold  float64 `json:"threshold"`
}

func notifyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req NotifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 受け取ったデータをログに出力
	log.Printf("利益検出: %s %s 利益率 %.2f%% (%.2f USDT) (volatility=%.2f%%, slippage=%.4f, trend=%s, threshold=%.2f%%)",
		req.Triangle, req.Direction, req.ProfitRate, req.ProfitUSDT, req.Volatility, req.Slippage, req.Trend, req.Threshold)

	// 成功レスポンスを返す
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "通知を受け取りました")
}

func main() {
	http.HandleFunc("/notify", notifyHandler)
	log.Println("Go サーバーをポート 8080 で起動します...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("サーバー起動エラー: %v", err)
	}
}
