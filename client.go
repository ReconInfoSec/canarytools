package canarytools

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	log "github.com/sirupsen/logrus"
)

// Client is a canarytools client, which is used to issue requests to the API
type Client struct {
	domain        string
	apikey        string
	baseURL       *url.URL
	httpclient    *http.Client
	l             *log.Logger
	lastCheck     time.Time
	errorCount    int
	fetchInterval int
}

// NewClient creates a new client from domain & API Key
func NewClient(domain, apikey string, fetchInterval int, l *log.Logger) (c *Client, err error) {
	c = &Client{}
	c.l = l

	c.fetchInterval = fetchInterval
	c.httpclient = &http.Client{Timeout: 5 * time.Second} // TODO: provide ability to configure
	c.domain = domain
	c.apikey = apikey
	c.baseURL, err = url.Parse(fmt.Sprintf("https://%s.canary.tools/api/v1/", domain))
	if err != nil {
		return
	}
	// c.url = fmt.Sprintf("https://%s.canary.tools/api/v1/%%s?auth_token=%s", domain, apikey)

	c.l.Debug("pinging console...")
	err = c.Ping()
	return
}

// api constructs the full URL for API querying, it always adds the API auth
// token, and adds  optional parameters as needed.
func (c Client) api(endpoint string, params *url.Values) (fullURL *url.URL, err error) {
	if endpoint == "" {
		return nil, errors.New("API endpoint has not been provided")
	}

	// if no additional params has been provided, we have to construct one
	if params == nil {
		params = &url.Values{}
	}
	// always add auth token to list of values
	params.Add("auth_token", c.apikey)

	// adding the API endpoint to path
	fullURL, err = url.Parse(c.baseURL.String())
	if err != nil {
		return
	}
	fullURL.Path = path.Join(fullURL.Path, endpoint)

	// building the full query
	fullURL.RawQuery = params.Encode()
	return
}

// decodeResponse decodes reponses into target interfaces
func (c Client) decodeResponse(endpoint string, params *url.Values, target interface{}) (err error) {
	fullURL, err := c.api(endpoint, params)
	if err != nil {
		return
	}

	c.l.WithFields(log.Fields{
		"url": fullURL.String(), // TODO: remove sensitive data
	}).Debug("hitting API")
	resp, err := c.httpclient.Get(fullURL.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()

	return json.NewDecoder(resp.Body).Decode(target)
}

// Ping tests connection to the console, and validity of connection params
func (c Client) Ping() (err error) {
	var pr PingResponse
	err = c.decodeResponse("ping", nil, &pr)
	if err != nil {
		return
	}

	if pr.Result != "success" {
		return errors.New(pr.Message) // there will be a message, if it failed
	}

	return
}

// getDevices returns  devices
func (c Client) getDevices(which string) (devices []Device, err error) {
	var getdevicesresponse GetDevicesResponse
	err = c.decodeResponse("devices/"+which, nil, &getdevicesresponse)
	if err != nil {
		return
	}

	if getdevicesresponse.Result != "success" {
		return nil, errors.New(getdevicesresponse.Message) // there will be a message, if it failed
	}
	return getdevicesresponse.Devices, nil
}

// GetAllDevices returns all devices
func (c Client) GetAllDevices() (devices []Device, err error) {
	return c.getDevices("all")
}

// GetLiveDevices returns live devices
func (c Client) GetLiveDevices() (devices []Device, err error) {
	return c.getDevices("live")
}

// GetDeadDevices returns live devices
func (c Client) GetDeadDevices() (devices []Device, err error) {
	return c.getDevices("dead")
}

// GetUnacknowledgedIncidents returns all Unacknowledged Incidents since time
// secified, setting "since" to zero vaule (time.Time{}) returns all unack'd incidents,
func (c Client) GetUnacknowledgedIncidents(since time.Time) (incidents []Incident, err error) {
	var unackIncidents GetIncidentsResponse
	var ts string
	var tt time.Time

	// this API has an optional parameter (newer_than) which is Timestamp used
	// to filter returned incidents. All incidents created after this timestamp
	// will be returned.
	// Format: ‘yyyy-mm-dd-hh:mm:ss’
	if since.Equal(time.Time{}) {
		// dummy date, but definetly before any incident
		tt, _ = time.Parse(time.RFC3339, "2000-01-02T15:04:05Z")
	} else {
		tt = since
	}
	ts = tt.Format("2006-01-02-15:04:05")
	u := &url.Values{}
	u.Add("newer_than", ts)
	u.Add("shrink", "true")
	err = c.decodeResponse("incidents/unacknowledged", u, &unackIncidents)
	if err != nil {
		return
	}

	if unackIncidents.Result != "success" {
		return nil, errors.New(unackIncidents.Message) // there will be a message, if it failed
	}

	return unackIncidents.Incidents, nil
}

// Feed fetches incidents and feeds them to chan
func (c *Client) Feed(incidnetsChan chan<- Incident) {
	log.Debug("getting all unacked incidents")
	ticker := time.NewTicker(time.Duration(c.fetchInterval) * time.Second)

	for range ticker.C {
		// get all unacked incidents
		log.WithFields(log.Fields{
			"lastCheck": c.lastCheck,
		}).Debug("getting all unacked incidents")
		unackedInc, err := c.GetUnacknowledgedIncidents(c.lastCheck)
		if err != nil {
			log.Error(err) // TODO: fail gracefully
		}
		c.lastCheck = time.Now().UTC()
		log.Debugf("found total of %d unacked incidents", len(unackedInc))
		for _, v := range unackedInc {
			log.WithFields(log.Fields{
				"UpdatedID": v.UpdatedID,
			}).Debug(v.Summary)
			incidnetsChan <- v
		}
	}
}
