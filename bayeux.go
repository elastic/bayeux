package bayeux

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt"
)

type MaybeMsg struct {
	Err error
	Msg TriggerEvent
}

func (e MaybeMsg) Failed() bool { return e.Err != nil }

func (e MaybeMsg) Error() string { return e.Err.Error() }

// TriggerEvent describes an event received from Bayeaux Endpoint
type TriggerEvent struct {
	ClientID string `json:"clientId"`
	Data     struct {
		Event struct {
			CreatedDate time.Time `json:"createdDate"`
			ReplayID    int       `json:"replayId"`
			Type        string    `json:"type"`
		} `json:"event"`
		Object  json.RawMessage `json:"sobject"`
		Payload json.RawMessage `json:"payload"`
	} `json:"data,omitempty"`
	Channel    string `json:"channel"`
	Successful bool   `json:"successful,omitempty"`
}

// Status is the state of success and subscribed channels
type status struct {
	connected    bool
	clientID     string
	channels     []string
	connectCount int
}

func (st *status) connect() {
	st.connectCount++
}

func (st *status) disconnect() {
	st.connectCount--
}

type BayeuxHandshake []struct {
	Ext struct {
		Replay bool `json:"replay"`
	} `json:"ext"`
	MinimumVersion           string   `json:"minimumVersion"`
	ClientID                 string   `json:"clientId"`
	SupportedConnectionTypes []string `json:"supportedConnectionTypes"`
	Channel                  string   `json:"channel"`
	Version                  string   `json:"version"`
	Successful               bool     `json:"successful"`
}

type Subscription struct {
	ClientID     string `json:"clientId"`
	Channel      string `json:"channel"`
	Subscription string `json:"subscription"`
	Successful   bool   `json:"successful"`
}

type Credentials struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
	IssuedAt    int
	ID          string
	TokenType   string `json:"token_type"`
	Signature   string
}

func (c Credentials) bayeuxUrl() string {
	return c.InstanceURL + "/cometd/38.0"
}

type clientIDAndCookies struct {
	clientID string
	cookies  []*http.Cookie
}

type AuthenticationParameters struct {
	ClientID     string // consumer key from Salesforce (e.g. 3MVG9pRsdbjsbdjfm1I.fz3f7zBuH4xdKCJcM9B5XLgxXh2AFTmQmr8JMn1vsadjsadjjsadakd_C)
	ClientSecret string // consumer secret from Salesforce (e.g. E9FE118633BC7SGDADUHUE81F19C1D4529D09CB7231754AD2F2CA668400619)
	Username     string // Salesforce user email (e.g. salesforce.user@email.com)
	Password     string // Salesforce password
	TokenURL     string // Salesforce token endpoint (e.g. https://login.salesforce.com/services/oauth2/token)
	Path         string // Salesforce private key path
	IsJwt        bool   // Salesforce auth method identifier
	Audience     string // Salesforce authorization server’s URL for the audience value (e.g. https://login.salesforce.com or https://test.salesforce.com or https://site.force.com/customers)
}

// Bayeux struct allow for centralized storage of creds, ids, and cookies
type Bayeux struct {
	creds Credentials
	id    clientIDAndCookies
}

type Authentication struct {
	URLValues      *url.Values
	AuthParameters *AuthenticationParameters
}

var wg sync.WaitGroup
var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
var st = status{false, "", []string{}, 0}

// newHTTPRequest is to create requests with context
func (b *Bayeux) newHTTPRequest(ctx context.Context, body string, route string) (*http.Request, error) {
	var jsonStr = []byte(body)
	req, err := http.NewRequest("POST", route, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, fmt.Errorf("bad Call request: %w", err)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		req = req.WithContext(ctx)

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b.creds.AccessToken))
		// Per Stackexchange comment, passing back cookies is required though undocumented in Salesforce API
		// We were unable to get process working without passing cookies back to SF server.
		// SF Reference: https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/intro_client_specs.htm
		for _, cookie := range b.id.cookies {
			req.AddCookie(cookie)
		}
	}
	return req, nil
}

// Call is the base function for making bayeux requests
func (b *Bayeux) call(ctx context.Context, body string, route string) (resp *http.Response, e error) {
	req, err := b.newHTTPRequest(ctx, body, route)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err = client.Do(req)
	if err == io.EOF {
		// Right way to handle EOF?
		return nil, fmt.Errorf("bad bayeuxCall io.EOF: %w", err)
	} else if err != nil {
		return nil, fmt.Errorf("bad unrecoverable call: %w", err)
	}
	return resp, nil
}

func (b *Bayeux) getClientID(ctx context.Context) error {
	handshake := `{"channel": "/meta/handshake", "supportedConnectionTypes": ["long-polling"], "version": "1.0"}`
	// Stub out clientIDAndCookies for first bayeuxCall
	resp, err := b.call(ctx, handshake, b.creds.bayeuxUrl())
	if err != nil {
		return fmt.Errorf("cannot get client id: %s", err)
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var h BayeuxHandshake
	if err := decoder.Decode(&h); err == io.EOF {
		return err
	} else if err != nil {
		return err
	}
	creds := clientIDAndCookies{h[0].ClientID, resp.Cookies()}
	b.id = creds
	return nil
}

// ReplayAll replay for past 24 hrs
const ReplayAll = -2

// ReplayNone start playing events at current moment
const ReplayNone = -1

// Replay accepts the following values
// Value
// -2: replay all events from past 24 hrs
// -1: start at current
// >= 0: start from this event number
type Replay struct {
	Value int
}

func (b *Bayeux) subscribe(ctx context.Context, channel string, replay string) error {
	handshake := fmt.Sprintf(`{
								"channel": "/meta/subscribe",
								"subscription": "%s",
								"clientId": "%s",
								"ext": {
									"replay": {"%s": "%s"}
									}
								}`, channel, b.id.clientID, channel, replay)
	resp, err := b.call(ctx, handshake, b.creds.bayeuxUrl())
	if err != nil {
		return fmt.Errorf("cannot subscribe: %w", err)
	}

	defer resp.Body.Close()
	if os.Getenv("DEBUG") != "" {
		logger.Printf("Response: %+v", resp)
		var b []byte
		if resp.Body != nil {
			b, _ = ioutil.ReadAll(resp.Body)
		}
		// Restore the io.ReadCloser to its original state
		resp.Body = ioutil.NopCloser(bytes.NewBuffer(b))
		// Use the content
		s := string(b)
		logger.Printf("Response Body: %s", s)
	}

	if resp.StatusCode > 299 {
		return fmt.Errorf("received non 2XX response: %w", err)
	}
	decoder := json.NewDecoder(resp.Body)
	var h []Subscription
	if err := decoder.Decode(&h); err == io.EOF {
		return err
	} else if err != nil {
		return err
	}
	sub := &h[0]
	st.connected = sub.Successful
	st.clientID = sub.ClientID
	st.channels = append(st.channels, channel)
	st.connect()
	if os.Getenv("DEBUG") != "" {
		logger.Printf("Established connection(s): %+v", st)
	}
	return nil
}

func (b *Bayeux) connect(ctx context.Context, out chan MaybeMsg) chan MaybeMsg {
	var waitMsgs sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			waitMsgs.Wait()
			close(out)
			st.disconnect()
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				postBody := fmt.Sprintf(`{"channel": "/meta/connect", "connectionType": "long-polling", "clientId": "%s"} `, b.id.clientID)
				resp, err := b.call(ctx, postBody, b.creds.bayeuxUrl())
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					out <- MaybeMsg{Err: fmt.Errorf("cannot connect to bayeux: %s, trying again", err)}
				} else {
					if os.Getenv("DEBUG") != "" {
						var b []byte
						if resp.Body != nil {
							b, _ = ioutil.ReadAll(resp.Body)
						}
						// Restore the io.ReadCloser to its original state
						resp.Body = ioutil.NopCloser(bytes.NewBuffer(b))
						// Use the content
						s := string(b)
						logger.Printf("Response Body: %s", s)
					}
					var x []TriggerEvent
					decoder := json.NewDecoder(resp.Body)
					if err := decoder.Decode(&x); err != nil && err == io.EOF {
						out <- MaybeMsg{Err: err}
						return
					}
					for i := range x {
						waitMsgs.Add(1)
						go func(e TriggerEvent) {
							defer waitMsgs.Done()
							out <- MaybeMsg{Msg: e}
						}(x[i])
					}
				}
			}
		}
	}()
	return out
}

// GetConnectedCount returns count of subcriptions
func GetConnectedCount() int {
	return st.connectCount
}

func GetSalesforceCredentials(auth Authentication) (creds *Credentials, err error) {
	var params *Authentication

	if auth.AuthParameters == nil {
		return nil, fmt.Errorf("auth parameters are empty")
	}
	if auth.AuthParameters.IsJwt {
		params, err = GetJWTAuthentication(*auth.AuthParameters)
		if err != nil {
			return nil, err
		}
	} else {

		if auth.AuthParameters.ClientID == "" || auth.AuthParameters.ClientSecret == "" || auth.AuthParameters.Username == "" || auth.AuthParameters.Password == "" {
			return nil, fmt.Errorf("missing required authentication parameters")
		}
		params, _ = GetClientCredentialAuthentication(auth.AuthParameters.ClientID, auth.AuthParameters.ClientSecret, auth.AuthParameters.Username, auth.AuthParameters.Password, auth.AuthParameters.TokenURL)
	}
	if auth.AuthParameters.TokenURL == "" {
		return nil, fmt.Errorf("missing required authentication parameter: token_url")
	}

	res, err := http.PostForm(auth.AuthParameters.TokenURL, *params.URLValues)
	if err != nil {
		return nil, fmt.Errorf("error posting form: %w", err)
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&creds); err == io.EOF {
		return nil, fmt.Errorf("error decoding response: %w", err)
	} else if err != nil {
		return nil, err
	} else if creds.AccessToken == "" {
		return nil, fmt.Errorf("unable to fetch access token: %w", err)
	}
	return creds, nil
}

// GetJWTAuthentication prepares the authentication parameters for JWT-based authentication
func GetJWTAuthentication(ap AuthenticationParameters) (*Authentication, error) {
	// Define your JWT claims (payload)
	claims := jwt.MapClaims{
		"iss": ap.ClientID,                          // Issuer
		"sub": ap.Username,                          // Subject
		"aud": ap.Audience,                          // Audience
		"exp": time.Now().Add(1 * time.Hour).Unix(), // Expiration time (1 hour)
	}

	// Load your private key (RSA key) used for signing
	privateKey, err := loadPrivateKey(ap.Path)
	if err != nil {
		return nil, fmt.Errorf("Error loading private key: %w", err)
	}

	// Create a new JWT token
	tokenString, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
	if err != nil {
		return nil, fmt.Errorf("Error signing JWT token: %w", err)
	}

	return &Authentication{
		URLValues: &url.Values{
			"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
			"assertion":  {tokenString},
		},
		AuthParameters: &AuthenticationParameters{
			ClientSecret: ap.ClientSecret,
			Username:     ap.Username,
			Audience:     ap.Audience,
			Path:         ap.Path,
		},
	}, nil
}

// GetClientCredentialAuthentication prepares the authentication parameters for client credential-based authentication
func GetClientCredentialAuthentication(clientId, clientSecret, username, password, tokenUrl string) (*Authentication, error) {
	if clientId != "" && clientSecret != "" && username != "" && password != "" && tokenUrl != "" {
		return nil, errors.New("all authentication parameters must be set")
	}

	return &Authentication{
		URLValues: &url.Values{
			"grant_type":    {"password"},
			"client_id":     {clientId},
			"client_secret": {clientSecret},
			"username":      {username},
			"password":      {password},
		},
		AuthParameters: &AuthenticationParameters{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			Username:     username,
			Password:     password,
			TokenURL:     tokenUrl,
		},
	}, nil
}
func loadPrivateKey(keyFile string) (*rsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (b *Bayeux) Channel(ctx context.Context, out chan MaybeMsg, r string, creds Credentials, channel string) chan MaybeMsg {
	b.creds = creds
	err := b.getClientID(ctx)
	if err != nil {
		out <- MaybeMsg{Err: err}
		close(out)
		return out
	}
	err = b.subscribe(ctx, channel, r)
	if err != nil {
		out <- MaybeMsg{Err: err}
		close(out)
		return out
	}
	c := b.connect(ctx, out)
	return c
}
