package oauth2x

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ccfos/nightingale/v6/storage"

	"github.com/toolkits/pkg/logger"

	"github.com/google/uuid"
	//jsoniter "github.com/json-iterator/go"
	"golang.org/x/oauth2"
)

type SsoClient struct {
	Enable          bool
	Config          oauth2.Config
	SsoAddr         string
	UserInfoAddr    string
	TranTokenMethod string
	CallbackAddr    string
	DisplayName     string
	CoverAttributes bool
	Attributes      struct {
		Username string
		Nickname string
		Phone    string
		Email    string
	}
	UserinfoIsArray bool
	UserinfoPrefix  string
	DefaultRoles    []string

	Ctx context.Context
	sync.RWMutex
}

type Config struct {
	Enable          bool
	DisplayName     string
	RedirectURL     string
	SsoAddr         string
	TokenAddr       string
	UserInfoAddr    string
	TranTokenMethod string
	ClientId        string
	ClientSecret    string
	CoverAttributes bool
	SkipTlsVerify   bool
	Attributes      struct {
		Username string
		Nickname string
		Phone    string
		Email    string
	}
	DefaultRoles    []string
	UserinfoIsArray bool
	UserinfoPrefix  string
	Scopes          []string
}

func New(cf Config) *SsoClient {
	var s = &SsoClient{}
	if !cf.Enable {
		return s
	}
	s.Reload(cf)
	return s
}

func (s *SsoClient) Reload(cf Config) {
	s.Lock()
	defer s.Unlock()
	if !cf.Enable {
		s.Enable = cf.Enable
		return
	}

	s.Enable = cf.Enable
	s.SsoAddr = cf.SsoAddr
	s.UserInfoAddr = cf.UserInfoAddr
	s.TranTokenMethod = cf.TranTokenMethod
	s.CallbackAddr = cf.RedirectURL
	s.DisplayName = cf.DisplayName
	s.CoverAttributes = cf.CoverAttributes
	s.Attributes.Username = cf.Attributes.Username
	s.Attributes.Nickname = cf.Attributes.Nickname
	s.Attributes.Phone = cf.Attributes.Phone
	s.Attributes.Email = cf.Attributes.Email
	s.UserinfoIsArray = cf.UserinfoIsArray
	s.UserinfoPrefix = cf.UserinfoPrefix
	s.DefaultRoles = cf.DefaultRoles

	s.Ctx = context.Background()

	if cf.SkipTlsVerify {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}

		// Create an HTTP client that uses our custom transport
		client := &http.Client{Transport: transport}
		s.Ctx = context.WithValue(s.Ctx, oauth2.HTTPClient, client)
	}

	s.Config = oauth2.Config{
		ClientID:     cf.ClientId,
		ClientSecret: cf.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  cf.SsoAddr,
			TokenURL: cf.TokenAddr,
		},
		RedirectURL: cf.RedirectURL,
		Scopes:      cf.Scopes,
	}
}

func (s *SsoClient) GetDisplayName() string {
	s.RLock()
	defer s.RUnlock()
	if !s.Enable {
		return ""
	}

	return s.DisplayName
}

func wrapStateKey(key string) string {
	return "n9e_oauth_" + key
}

// Authorize return the sso authorize location with state
func (s *SsoClient) Authorize(redis storage.Redis, redirect string) (string, error) {
	state := uuid.New().String()
	ctx := context.Background()

	err := redis.Set(ctx, wrapStateKey(state), redirect, time.Duration(300*time.Second)).Err()
	if err != nil {
		return "", err
	}

	s.RLock()
	defer s.RUnlock()
	return s.Config.AuthCodeURL(state), nil
}

func fetchRedirect(redis storage.Redis, ctx context.Context, state string) (string, error) {
	return redis.Get(ctx, wrapStateKey(state)).Result()
}

func deleteRedirect(redis storage.Redis, ctx context.Context, state string) error {
	return redis.Del(ctx, wrapStateKey(state)).Err()
}

// Callback 用 code 兑换 accessToken 以及 用户信息
func (s *SsoClient) Callback(redis storage.Redis, ctx context.Context, code, state string) (*CallbackOutput, error) {
	ret, err := s.exchangeUser(code)
	if err != nil {
		return nil, fmt.Errorf("ilegal user:%v", err)
	}
	ret.Redirect, err = fetchRedirect(redis, ctx, state)
	if err != nil {
		logger.Errorf("get redirect err:%v code:%s state:%s", code, state, err)
	}

	err = deleteRedirect(redis, ctx, state)
	if err != nil {
		logger.Errorf("delete redirect err:%v code:%s state:%s", code, state, err)
	}
	return ret, nil
}

type CallbackOutput struct {
	Redirect    string `json:"redirect"`
	Msg         string `json:"msg"`
	AccessToken string `json:"accessToken"`
	Username    string `json:"Username"`
	Nickname    string `json:"Nickname"`
	Phone       string `yaml:"Phone"`
	Email       string `yaml:"Email"`
}

func (s *SsoClient) exchangeUser(code string) (*CallbackOutput, error) {
	s.RLock()
	defer s.RUnlock()

	oauth2Token, err := s.Config.Exchange(s.Ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %s", err)
	}
	userInfo, err := s.getUserInfo(s.UserInfoAddr, oauth2Token.AccessToken, s.TranTokenMethod)
	if err != nil {
		logger.Errorf("failed to get user info: %s", err)
		return nil, fmt.Errorf("failed to get user info: %s", err)
	}
	logger.Debugf("get userInfo: %s", string(userInfo))
	return &CallbackOutput{
		AccessToken: oauth2Token.AccessToken,
		Username:    getUserinfoField(userInfo, s.UserinfoIsArray, s.UserinfoPrefix, s.Attributes.Username),
		Nickname:    getUserinfoField(userInfo, s.UserinfoIsArray, s.UserinfoPrefix, s.Attributes.Nickname),
		Phone:       getUserinfoField(userInfo, s.UserinfoIsArray, s.UserinfoPrefix, s.Attributes.Phone),
		Email:       getUserinfoField(userInfo, s.UserinfoIsArray, s.UserinfoPrefix, s.Attributes.Email),
	}, nil
}

func (s *SsoClient) getUserInfo(UserInfoAddr, accessToken string, TranTokenMethod string) ([]byte, error) {
	var req *http.Request
	if TranTokenMethod == "formdata" {
		body := bytes.NewBuffer([]byte("access_token=" + accessToken))
		r, err := http.NewRequest("POST", UserInfoAddr, body)
		if err != nil {
			return nil, err
		}
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req = r
	} else if TranTokenMethod == "querystring" {
		r, err := http.NewRequest("GET", UserInfoAddr+"?access_token="+accessToken, nil)
		if err != nil {
			return nil, err
		}
		r.Header.Add("Authorization", "Bearer "+accessToken)
		req = r
	} else {
		r, err := http.NewRequest("GET", UserInfoAddr, nil)
		if err != nil {
			return nil, err
		}
		r.Header.Add("Authorization", "Bearer "+accessToken)
		req = r
	}

	client := http.DefaultClient
	c := s.Ctx.Value(oauth2.HTTPClient)
	if c != nil {
		client = c.(*http.Client)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	return body, err
}

func getUserinfoField(input []byte, isArray bool, prefix, field string) string {
	// 创建一个空接口变量来存储解析后的 JSON 数据
	var jsonData interface{}

	// 使用标准库的 json.Unmarshal 将输入的 JSON 字节流解析为 jsonData
	if err := json.Unmarshal(input, &jsonData); err != nil {
		return ""
	}

	// 用于存储最终字段值的空接口变量
	var fieldValue interface{}

	// 根据传入的参数进行逻辑处理
	if prefix == "" {
		if isArray {
			// 如果是数组，我们可以假设 jsonData 是一个 []interface{}
			// 我们获取索引为 0 的元素，然后将其转换为 map[string]interface{}
			// 接着我们可以从 map 中获取指定的字段值
			fieldValue = jsonData.([]interface{})[0].(map[string]interface{})[field]
		} else {
			// 如果不是数组，我们假设 jsonData 是一个 map[string]interface{}
			// 然后我们直接从 map 中获取指定的字段值
			fieldValue = jsonData.(map[string]interface{})[field]
		}
	} else {
		if isArray {
			// 类似上面的逻辑，我们首先获取 prefix 对应的数组
			// 然后从数组中获取索引为 0 的元素，将其转换为 map[string]interface{}
			// 最后从 map 中获取指定的字段值
			fieldValue = jsonData.(map[string]interface{})[prefix].([]interface{})[0].(map[string]interface{})[field]
		} else {
			// 类似上面的逻辑，我们首先获取 prefix 对应的 map
			// 然后从 map 中获取指定的字段值
			fieldValue = jsonData.(map[string]interface{})[prefix].(map[string]interface{})[field]
		}
	}

	// 如果字段值为空，返回空字符串
	if fieldValue == nil {
		return ""
	}

	// 将字段值转换为字符串并返回
	return fieldValue.(string)
}

/*
func getUserinfoField(input []byte, isArray bool, prefix, field string) string {
	if prefix == "" {
		if isArray {
			return jsoniter.Get(input, 0).Get(field).ToString()
		} else {
			return jsoniter.Get(input, field).ToString()
		}
	} else {
		if isArray {
			return jsoniter.Get(input, prefix, 0).Get(field).ToString()
		} else {
			return jsoniter.Get(input, prefix).Get(field).ToString()
		}
	}
}
*/
