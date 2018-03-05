package huobi

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/gorilla/websocket"
	. "github.com/thbourlove/GoEx"
)

type HuoBi_V2 struct {
	httpClient *http.Client
	accountId,
	baseUrl,
	accessKey,
	secretKey string
	c              *websocket.Conn
	depthChanMap   map[string]chan *Depth
	tradeChanMap   map[string]chan []Trade
	channelMap     map[string]int
	writeMsgChan   chan interface{}
	writeErrorChan chan error
}

const (
	DEPTH_CHANNEL = iota
	TRADE_CHANNEL
)

type response struct {
	Status  string          `json:"status"`
	Data    json.RawMessage `json:"data"`
	Errmsg  string          `json:"err-msg"`
	Errcode string          `json:"err-code"`
}

func NewV2(client *http.Client, accessKey, secretKey string) *HuoBi_V2 {
	return &HuoBi_V2{
		httpClient: client,
		accessKey:  accessKey,
		secretKey:  secretKey,
		baseUrl:    "https://be.huobi.com",

		tradeChanMap:   map[string]chan []Trade{},
		depthChanMap:   map[string]chan *Depth{},
		channelMap:     map[string]int{},
		writeMsgChan:   make(chan interface{}),
		writeErrorChan: make(chan error),
	}
}

//func NewV2(httpClient *http.Client, accessKey, secretKey, clientId string) *HuoBi_V2 {
//return &HuoBi_V2{httpClient, clientId, "https://be.huobi.com", accessKey, secretKey}
//}

func (hbV2 *HuoBi_V2) GetAccountId() (string, error) {
	path := "/v1/account/accounts"
	params := &url.Values{}
	hbV2.buildPostForm("GET", path, params)

	//log.Println(hbV2.baseUrl + path + "?" + params.Encode())

	respmap, err := HttpGet(hbV2.httpClient, hbV2.baseUrl+path+"?"+params.Encode())
	if err != nil {
		return "", err
	}
	//log.Println(respmap)
	if respmap["status"].(string) != "ok" {
		return "", errors.New(respmap["err-code"].(string))
	}

	data := respmap["data"].([]interface{})
	accountIdMap := data[0].(map[string]interface{})
	hbV2.accountId = fmt.Sprintf("%.f", accountIdMap["id"].(float64))

	//log.Println(respmap)
	return hbV2.accountId, nil
}

func (hbV2 *HuoBi_V2) GetAccount() (*Account, error) {
	path := fmt.Sprintf("/v1/account/accounts/%s/balance", hbV2.accountId)
	params := &url.Values{}
	params.Set("accountId-id", hbV2.accountId)
	hbV2.buildPostForm("GET", path, params)

	urlStr := hbV2.baseUrl + path + "?" + params.Encode()
	//println(urlStr)
	respmap, err := HttpGet(hbV2.httpClient, urlStr)

	if err != nil {
		return nil, err
	}

	//log.Println(respmap)

	if respmap["status"].(string) != "ok" {
		return nil, errors.New(respmap["err-code"].(string))
	}

	datamap := respmap["data"].(map[string]interface{})
	if datamap["state"].(string) != "working" {
		return nil, errors.New(datamap["state"].(string))
	}

	list := datamap["list"].([]interface{})
	acc := new(Account)
	acc.SubAccounts = make(map[Currency]SubAccount, 6)
	acc.Exchange = hbV2.GetExchangeName()

	subAccMap := make(map[Currency]*SubAccount)

	for _, v := range list {
		balancemap := v.(map[string]interface{})
		currencySymbol := balancemap["currency"].(string)
		currency := NewCurrency(currencySymbol, "")
		typeStr := balancemap["type"].(string)
		balance := ToFloat64(balancemap["balance"])
		if subAccMap[currency] == nil {
			subAccMap[currency] = new(SubAccount)
		}
		subAccMap[currency].Currency = currency
		switch typeStr {
		case "trade":
			subAccMap[currency].Amount = balance
		case "frozen":
			subAccMap[currency].ForzenAmount = balance
		}
	}

	for k, v := range subAccMap {
		acc.SubAccounts[k] = *v
	}

	return acc, nil
}

func (hbV2 *HuoBi_V2) placeOrder(amount, price string, pair CurrencyPair, orderType string) (string, error) {
	path := "/v1/order/orders/place"
	params := url.Values{}
	params.Set("account-id", hbV2.accountId)
	params.Set("amount", amount)
	params.Set("symbol", strings.ToLower(pair.ToSymbol("")))
	params.Set("type", orderType)

	switch orderType {
	case "buy-limit", "sell-limit":
		params.Set("price", price)
	}

	hbV2.buildPostForm("POST", path, &params)

	resp, err := HttpPostForm3(hbV2.httpClient, hbV2.baseUrl+path+"?"+params.Encode(), hbV2.toJson(params),
		map[string]string{"Content-Type": "application/json", "Accept-Language": "zh-cn"})
	if err != nil {
		return "", err
	}

	respmap := make(map[string]interface{})
	err = json.Unmarshal(resp, &respmap)
	if err != nil {
		return "", err
	}

	if respmap["status"].(string) != "ok" {
		return "", errors.New(respmap["err-code"].(string))
	}

	return respmap["data"].(string), nil
}

func (hbV2 *HuoBi_V2) LimitBuy(amount, price string, currency CurrencyPair) (*Order, error) {
	orderId, err := hbV2.placeOrder(amount, price, currency, "buy-limit")
	if err != nil {
		return nil, err
	}
	return &Order{
		Currency: currency,
		OrderID:  ToInt(orderId),
		Amount:   ToFloat64(amount),
		Price:    ToFloat64(price),
		Side:     BUY}, nil
}

func (hbV2 *HuoBi_V2) LimitSell(amount, price string, currency CurrencyPair) (*Order, error) {
	orderId, err := hbV2.placeOrder(amount, price, currency, "sell-limit")
	if err != nil {
		return nil, err
	}
	return &Order{
		Currency: currency,
		OrderID:  ToInt(orderId),
		Amount:   ToFloat64(amount),
		Price:    ToFloat64(price),
		Side:     SELL}, nil
}

func (hbV2 *HuoBi_V2) MarketBuy(amount, price string, currency CurrencyPair) (*Order, error) {
	orderId, err := hbV2.placeOrder(amount, price, currency, "buy-market")
	if err != nil {
		return nil, err
	}
	return &Order{
		Currency: currency,
		OrderID:  ToInt(orderId),
		Amount:   ToFloat64(amount),
		Price:    ToFloat64(price),
		Side:     BUY_MARKET}, nil
}

func (hbV2 *HuoBi_V2) MarketSell(amount, price string, currency CurrencyPair) (*Order, error) {
	orderId, err := hbV2.placeOrder(amount, price, currency, "sell-market")
	if err != nil {
		return nil, err
	}
	return &Order{
		Currency: currency,
		OrderID:  ToInt(orderId),
		Amount:   ToFloat64(amount),
		Price:    ToFloat64(price),
		Side:     SELL_MARKET}, nil
}

func (hbV2 *HuoBi_V2) parseOrder(ordmap map[string]interface{}) Order {
	ord := Order{
		OrderID:    ToInt(ordmap["id"]),
		Amount:     ToFloat64(ordmap["amount"]),
		Price:      ToFloat64(ordmap["price"]),
		DealAmount: ToFloat64(ordmap["field-amount"]),
		Fee:        ToFloat64(ordmap["field-fees"]),
		OrderTime:  ToInt(ordmap["created-at"]),
	}

	state := ordmap["state"].(string)
	switch state {
	case "submitted":
		ord.Status = ORDER_UNFINISH
	case "filled":
		ord.Status = ORDER_FINISH
	case "partial-filled":
		ord.Status = ORDER_PART_FINISH
	case "canceled", "partial-canceled":
		ord.Status = ORDER_CANCEL
	default:
		ord.Status = ORDER_UNFINISH
	}

	if ord.DealAmount > 0.0 {
		ord.AvgPrice = ToFloat64(ordmap["field-cash-amount"]) / ord.DealAmount
	}

	typeS := ordmap["type"].(string)
	switch typeS {
	case "buy-limit":
		ord.Side = BUY
	case "buy-market":
		ord.Side = BUY_MARKET
	case "sell-limit":
		ord.Side = SELL
	case "sell-market":
		ord.Side = SELL_MARKET
	}
	return ord
}

func (hbV2 *HuoBi_V2) GetOneOrder(orderId string, currency CurrencyPair) (*Order, error) {
	path := "/v1/order/orders/" + orderId
	params := url.Values{}
	hbV2.buildPostForm("GET", path, &params)
	respmap, err := HttpGet(hbV2.httpClient, hbV2.baseUrl+path+"?"+params.Encode())
	if err != nil {
		return nil, err
	}

	if respmap["status"].(string) != "ok" {
		return nil, errors.New(respmap["err-code"].(string))
	}

	datamap := respmap["data"].(map[string]interface{})
	order := hbV2.parseOrder(datamap)
	order.Currency = currency
	//log.Println(respmap)
	return &order, nil
}

func (hbV2 *HuoBi_V2) GetUnfinishOrders(currency CurrencyPair) ([]Order, error) {
	return hbV2.getOrders(queryOrdersParams{
		pair:   currency,
		states: "pre-submitted,submitted,partial-filled",
		size:   100,
		//direct:""
	})
}

func (hbV2 *HuoBi_V2) CancelOrder(orderId string, currency CurrencyPair) (bool, error) {
	path := fmt.Sprintf("/v1/order/orders/%s/submitcancel", orderId)
	params := url.Values{}
	hbV2.buildPostForm("POST", path, &params)
	resp, err := HttpPostForm3(hbV2.httpClient, hbV2.baseUrl+path+"?"+params.Encode(), hbV2.toJson(params),
		map[string]string{"Content-Type": "application/json", "Accept-Language": "zh-cn"})
	if err != nil {
		return false, err
	}

	var respmap map[string]interface{}
	err = json.Unmarshal(resp, &respmap)
	if err != nil {
		return false, err
	}

	if respmap["status"].(string) != "ok" {
		return false, errors.New(string(resp))
	}

	return true, nil
}

func (hbV2 *HuoBi_V2) GetOrderHistorys(currency CurrencyPair, currentPage, pageSize int) ([]Order, error) {
	return hbV2.getOrders(queryOrdersParams{
		pair:   currency,
		size:   pageSize,
		states: "partial-canceled,filled",
		direct: "next",
	})
}

type queryOrdersParams struct {
	types,
	startDate,
	endDate,
	states,
	from,
	direct string
	size int
	pair CurrencyPair
}

func (hbV2 *HuoBi_V2) getOrders(queryparams queryOrdersParams) ([]Order, error) {
	path := "/v1/order/orders"
	params := url.Values{}
	params.Set("symbol", strings.ToLower(queryparams.pair.ToSymbol("")))
	params.Set("states", queryparams.states)

	if queryparams.direct != "" {
		params.Set("direct", queryparams.direct)
	}

	if queryparams.size > 0 {
		params.Set("size", fmt.Sprint(queryparams.size))
	}

	hbV2.buildPostForm("GET", path, &params)
	respmap, err := HttpGet(hbV2.httpClient, fmt.Sprintf("%s%s?%s", hbV2.baseUrl, path, params.Encode()))
	if err != nil {
		return nil, err
	}

	if respmap["status"].(string) != "ok" {
		return nil, errors.New(respmap["err-code"].(string))
	}

	datamap := respmap["data"].([]interface{})
	var orders []Order
	for _, v := range datamap {
		ordmap := v.(map[string]interface{})
		ord := hbV2.parseOrder(ordmap)
		ord.Currency = queryparams.pair
		orders = append(orders, ord)
	}

	return orders, nil
}

func (hbV2 *HuoBi_V2) GetExchangeName() string {
	return "huobi.com"
}

func (hbV2 *HuoBi_V2) GetTicker(currencyPair CurrencyPair) (*Ticker, error) {
	url := hbV2.baseUrl + "/market/detail/merged?symbol=" + strings.ToLower(currencyPair.ToSymbol(""))
	respmap, err := HttpGet(hbV2.httpClient, url)
	if err != nil {
		return nil, err
	}

	if respmap["status"].(string) == "error" {
		return nil, errors.New(respmap["err-msg"].(string))
	}

	tickmap, ok := respmap["tick"].(map[string]interface{})
	if !ok {
		return nil, errors.New("tick assert error")
	}

	ticker := new(Ticker)
	ticker.Vol = ToFloat64(tickmap["amount"])
	ticker.Low = ToFloat64(tickmap["low"])
	ticker.High = ToFloat64(tickmap["high"])
	bid, isOk := tickmap["bid"].([]interface{})
	if isOk != true {
		return nil, errors.New("no bid")
	}
	ask, isOk := tickmap["ask"].([]interface{})
	if isOk != true {
		return nil, errors.New("no ask")
	}
	ticker.Buy = ToFloat64(bid[0])
	ticker.Sell = ToFloat64(ask[0])
	ticker.Last = ToFloat64(tickmap["close"])
	ticker.Date = ToUint64(respmap["ts"])

	return ticker, nil
}

func (hbV2 *HuoBi_V2) GetDepth(size int, currency CurrencyPair) (*Depth, error) {
	url := hbV2.baseUrl + "/market/depth?symbol=%s&type=step0"
	respmap, err := HttpGet(hbV2.httpClient, fmt.Sprintf(url, strings.ToLower(currency.ToSymbol(""))))
	if err != nil {
		return nil, err
	}

	if "ok" != respmap["status"].(string) {
		return nil, errors.New(respmap["err-msg"].(string))
	}

	tick, _ := respmap["tick"].(map[string]interface{})
	bids, _ := tick["bids"].([]interface{})
	asks, _ := tick["asks"].([]interface{})

	depth := new(Depth)
	_size := size
	for _, r := range asks {
		var dr DepthRecord
		rr := r.([]interface{})
		dr.Price = ToFloat64(rr[0])
		dr.Amount = ToFloat64(rr[1])
		depth.AskList = append(depth.AskList, dr)

		_size--
		if _size == 0 {
			break
		}
	}

	_size = size
	for _, r := range bids {
		var dr DepthRecord
		rr := r.([]interface{})
		dr.Price = ToFloat64(rr[0])
		dr.Amount = ToFloat64(rr[1])
		depth.BidList = append(depth.BidList, dr)

		_size--
		if _size == 0 {
			break
		}
	}

	return depth, nil
}

func (hbV2 *HuoBi_V2) GetKlineRecords(currency CurrencyPair, period, size, since int) ([]Kline, error) {
	panic("not implement")
}

//非个人，整个交易所的交易记录
func (hbV2 *HuoBi_V2) GetTrades(currencyPair CurrencyPair, since int64) ([]Trade, error) {
	panic("not implement")
}

func (hbV2 *HuoBi_V2) buildPostForm(reqMethod, path string, postForm *url.Values) error {
	postForm.Set("AccessKeyId", hbV2.accessKey)
	postForm.Set("SignatureMethod", "HmacSHA256")
	postForm.Set("SignatureVersion", "2")
	postForm.Set("Timestamp", time.Now().UTC().Format("2006-01-02T15:04:05"))
	domain := strings.Replace(hbV2.baseUrl, "https://", "", len(hbV2.baseUrl))
	payload := fmt.Sprintf("%s\n%s\n%s\n%s", reqMethod, domain, path, postForm.Encode())
	sign, _ := GetParamHmacSHA256Base64Sign(hbV2.secretKey, payload)
	postForm.Set("Signature", sign)
	return nil
}

func (hbV2 *HuoBi_V2) toJson(params url.Values) string {
	parammap := make(map[string]string)
	for k, v := range params {
		parammap[k] = v[0]
	}
	jsonData, _ := json.Marshal(parammap)
	return string(jsonData)
}

func (hb *HuoBi_V2) connectWebsocket() error {
	url := "wss://api.huobi.pro/ws"
	var err error
	hb.c, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("websocket dial %s", url))
	}
	return nil
}

func (hb *HuoBi_V2) writeWebsocketMessage(msg interface{}) error {
	hb.writeMsgChan <- msg
	return <-hb.writeErrorChan
}

func (hb *HuoBi_V2) RunWebsocket() error {
	if err := hb.connectWebsocket(); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(time.Duration(5) * time.Second)
		for {
			select {
			case <-ticker.C:
				err := hb.c.WriteJSON(map[string]string{
					"event": "ping",
				})
				if err != nil {
					log.Printf("write ping: %v\n", err)
					if err := hb.connectWebsocket(); err != nil {
						log.Printf("reconnect websocket failed: %v\n", err)
					}
				}
			case msg := <-hb.writeMsgChan:
				hb.writeErrorChan <- hb.c.WriteJSON(msg)
			}
		}
	}()

	msgChan := make(chan map[string]interface{})

	go func() {
		for {
			_, reader, err := hb.c.NextReader()
			if err != nil {
				log.Printf("get next reader: %v\n", err)
				continue
			}
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				log.Printf("new gzip reader: %v\n", err)
				continue
			}
			jsonReader := json.NewDecoder(gzipReader)

			var msgmap map[string]interface{}
			if err := jsonReader.Decode(&msgmap); err != nil {
				log.Printf("decode json: %v\n", err)
				continue
			}

			if ping, isok := msgmap["ping"].(int64); isok {
				if err := hb.writeWebsocketMessage(map[string]int64{"pong": ping}); err != nil {
					log.Printf("write pong failed: %v\n", err)
				}
				continue
			}

			if _, isok := msgmap["pong"].(int64); isok {
				continue
			}

			if _, isok := msgmap["subbed"].(string); isok {
			}

			msgChan <- msgmap
		}
	}()

	go func() {
		for {
			select {
			case msgmap := <-msgChan:
				var isok bool
				var channel string
				var channelType int

				if channel, isok = msgmap["channel"].(string); !isok {
					log.Printf("miss channel, msg map: %v\n", msgmap)
					continue
				}

				if channel == "addChannel" {
					//log.Printf("add %s channel success\n", msgmap["data"].(map[string]interface{})["channel"])
					continue
				}

				if channelType, isok = hb.channelMap[channel]; !isok {
					log.Printf("receive unknown channel(%s) message(%v)\n", channel, msgmap)
					continue
				}

				if channelType == DEPTH_CHANNEL {
					d, err := dataToDepth(msgmap["data"])
					if err != nil {
						log.Printf("convert data to depth: %v\n", err)
						continue
					}
					hb.depthChanMap[channel] <- d
				} else if channelType == TRADE_CHANNEL {
					t, err := dataToTrades(msgmap["data"])
					if err != nil {
						log.Printf("convert data to trades: %v\n", err)
						continue
					}
					hb.tradeChanMap[channel] <- t
				}
			}
		}
	}()

	return nil
}

func dataToDepth(data interface{}) (*Depth, error) {
	var isok bool
	var bids, asks []interface{}
	var depthdata map[string]interface{}
	if depthdata, isok = data.(map[string]interface{}); !isok {
		return nil, errors.Errorf("data type is not map[string]interface{}: %v\n", data)
	}
	bids, _ = depthdata["bids"].([]interface{})
	asks, _ = depthdata["asks"].([]interface{})

	d := new(Depth)
	for _, r := range asks {
		var dr DepthRecord
		rr := r.([]interface{})
		dr.Price = ToFloat64(rr[0])
		dr.Amount = ToFloat64(rr[1])
		d.AskList = append(d.AskList, dr)
	}

	for _, r := range bids {
		var dr DepthRecord
		rr := r.([]interface{})
		dr.Price = ToFloat64(rr[0])
		dr.Amount = ToFloat64(rr[1])
		d.BidList = append(d.BidList, dr)
	}
	return d, nil
}

func dataToTrades(data interface{}) ([]Trade, error) {
	var tradesdata []interface{}
	var trades []Trade
	var isok bool
	if tradesdata, isok = data.([]interface{}); !isok {
		return nil, errors.Errorf("data type is not []interface{} %v\n", tradesdata)
	}

	for _, d := range tradesdata {
		dd := d.([]interface{})
		t := Trade{}
		t.Tid = ToInt64(dd[0])
		t.Price = ToFloat64(dd[1])
		t.Amount = ToFloat64(dd[2])

		now := time.Now()
		timeStr := dd[3].(string)
		timeMeta := strings.Split(timeStr, ":")
		h, _ := strconv.Atoi(timeMeta[0])
		m, _ := strconv.Atoi(timeMeta[1])
		s, _ := strconv.Atoi(timeMeta[2])
		//临界点处理
		if now.Hour() == 0 {
			if h <= 23 && h >= 20 {
				pre := now.AddDate(0, 0, -1)
				t.Date = time.Date(pre.Year(), pre.Month(), pre.Day(), h, m, s, 0, time.Local).Unix() * 1000
			} else if h == 0 {
				t.Date = time.Date(now.Year(), now.Month(), now.Day(), h, m, s, 0, time.Local).Unix() * 1000
			}
		} else {
			t.Date = time.Date(now.Year(), now.Month(), now.Day(), h, m, s, 0, time.Local).Unix() * 1000
		}

		t.Type = dd[4].(string)
		trades = append(trades, t)
	}
	return trades, nil
}

func (hb *HuoBi_V2) GetDepthChan(pair CurrencyPair) (chan *Depth, chan struct{}, error) {
	channel := fmt.Sprintf("market.%s.depth.%s", strings.ToLower(pair.ToSymbol("")), "step0")
	msg := map[string]interface{}{
		"sub": channel,
		"id":  "thb",
		//"pick": []string{"bids.100", "asks.100"},
	}

	depth := make(chan *Depth)
	hb.depthChanMap[channel] = depth
	hb.channelMap[channel] = DEPTH_CHANNEL

	if err := hb.writeWebsocketMessage(msg); err != nil {
		return nil, nil, errors.Wrapf(err, "write depth message: %v", msg)
	}

	done := make(chan struct{})
	return depth, done, nil

	go func() {
		defer hb.c.Close()
		defer close(done)
		for {
			select {
			default:
				_, reader, err := hb.c.NextReader()
				if err != nil {
					log.Printf("get next reader: %v\n", err)
					return
					continue
				}
				gzipReader, err := gzip.NewReader(reader)
				if err != nil {
					log.Printf("new gzip reader: %v\n", err)
					continue
				}
				jsonReader := json.NewDecoder(gzipReader)

				var msgmap map[string]interface{}
				if err := jsonReader.Decode(&msgmap); err != nil {
					log.Printf("decode json: %v\n", err)
					continue
				}

				tick, ok := msgmap["tick"].(map[string]interface{})
				if !ok {
					log.Printf("msg: %v\n", msgmap)
					continue
				}

				bids, _ := tick["bids"].([]interface{})
				asks, _ := tick["asks"].([]interface{})

				d := new(Depth)
				for _, r := range asks {
					var dr DepthRecord
					rr := r.([]interface{})
					dr.Price = ToFloat64(rr[0])
					dr.Amount = ToFloat64(rr[1])
					d.AskList = append(d.AskList, dr)
				}

				for _, r := range bids {
					var dr DepthRecord
					rr := r.([]interface{})
					dr.Price = ToFloat64(rr[0])
					dr.Amount = ToFloat64(rr[1])
					d.BidList = append(d.BidList, dr)
				}

				depth <- d
			}
		}
	}()

	return depth, done, nil
}
