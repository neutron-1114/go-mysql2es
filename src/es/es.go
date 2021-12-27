package es

import (
	"encoding/json"
	"fmt"
	"go-mysql2es/src/config"
	"io/ioutil"
	"net/http"
	"strings"
)

type Client struct {
	conf     *config.ESConf
	rule     *config.Rule
	client   *http.Client
	keyField string
}

func New(conf *config.Conf) *Client {
	return &Client{conf.ES, conf.Rule, &http.Client{}, fmt.Sprintf("%v.%v", conf.Rule.MainTable.TableName, conf.Rule.MainTable.MainCollName)}
}

func (c *Client) Upsert(data map[*config.Coll]interface{}) error {
	var id interface{}
	mappingParams := make(map[string]interface{})
	for k, v := range data {
		mappingParams[k.MappingName] = v
		if k.FullName == c.keyField {
			id = v
		}
	}
	url := fmt.Sprintf("http://%v:%v/%v/%v/%v", c.conf.Host, c.conf.Port, c.conf.Index, c.conf.Type, id)
	method := "PUT"
	dataStr, _ := json.Marshal(mappingParams)
	payload := strings.NewReader(string(dataStr))
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) BatchInsert(datas []map[*config.Coll]interface{}) error {
	var body []string
	for _, data := range datas {
		var id interface{}
		mappingParams := make(map[string]interface{})
		for k, v := range data {
			mappingParams[k.MappingName] = v
			if k.FullName == c.keyField {
				id = v
				mappingParams["id"] = id
			}
		}
		dataStr, _ := json.Marshal(mappingParams)
		body = append(body, fmt.Sprintf(`{"index":{"_id":%v}}`, id))
		body = append(body, string(dataStr))
	}
	url := fmt.Sprintf("http://%v:%v/%v/%v/_bulk", c.conf.Host, c.conf.Port, c.conf.Index, c.conf.Type)
	method := "POST"
	payload := strings.NewReader(strings.Join(body, "\n") + "\n")
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Update() error {
	return nil
}

func (c *Client) Delete(id interface{}) error {
	url := fmt.Sprintf("http://%v:%v/%v/%v/%v", c.conf.Host, c.conf.Port, c.conf.Index, c.conf.Type, id)
	method := "DELETE"
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Count() (uint64, error) {
	url := fmt.Sprintf("http://%v:%v/%v/_count", c.conf.Host, c.conf.Port, c.conf.Index)
	method := "GET"
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return 0, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != 200 {
		return 0, nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 0, err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return 0, err
	}
	return uint64(m["count"].(float64)), nil
}
