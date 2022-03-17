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
	host    string
	port    int
	index   string
	t       string
	idField string
	client  *http.Client
}

func (c *Client) SetIndex(index string) {
	c.index = index
}

func (c *Client) GetIndex() string {
	return c.index
}

func New(conf *config.Conf) *Client {
	return &Client{
		conf.ES.Host,
		conf.ES.Port,
		conf.ES.Index,
		conf.ES.Type,
		fmt.Sprintf("%v.%v", conf.Rule.MainTable.TableName, conf.Rule.MainTable.MainCollName),
		&http.Client{}}
}

func (c *Client) Upsert(data map[*config.Coll]interface{}) error {
	var id interface{}
	mappingParams := make(map[string]interface{})
	for k, v := range data {
		mappingParams[k.MappingName] = v
		if k.FullName == c.idField {
			id = v
		}
	}
	url := fmt.Sprintf("http://%v:%v/%v/%v/%v", c.host, c.port, c.index, c.t, id)
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
			if k.FullName == c.idField {
				id = v
				mappingParams["id"] = id
			}
		}
		dataStr, _ := json.Marshal(mappingParams)
		body = append(body, fmt.Sprintf(`{"index":{"_id":%v}}`, id))
		body = append(body, string(dataStr))
	}
	url := fmt.Sprintf("http://%v:%v/%v/%v/_bulk", c.host, c.port, c.index, c.t)
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
	url := fmt.Sprintf("http://%v:%v/%v/%v/%v", c.host, c.port, c.index, c.t, id)
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
	url := fmt.Sprintf("http://%v:%v/%v/_count", c.host, c.port, c.index)
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

func (c *Client) AliasAndClear(alias string) error {
	err := c.Alias(alias)
	if err != nil {
		return err
	}

}

func (c *Client) Alias(alias string) error {
	url := fmt.Sprintf("http://%v:%v/_aliases?pretty", c.host, c.port)
	method := "POST"
	j := fmt.Sprintf(`{"actions":[{"add":{"index":"%v","alias":"%v"}}]}`, c.index, alias)
	payload := strings.NewReader(j)
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
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

func (c *Client) GetAlias(alias string) error {
	url := fmt.Sprintf("http://%v:%v/_aliases?pretty", c.host, c.port)
	method := "POST"
	j := fmt.Sprintf(`{"actions":[{"add":{"index":"%v","alias":"%v"}}]}`, c.index, alias)
	payload := strings.NewReader(j)
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
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
