package canarytools

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	log "github.com/sirupsen/logrus"
)

// ChirpForwarder is the main struct of the forwarder
// it contains the configurations, and various components of the forwarder
type ChirpForwarder struct {
	// configs
	cfg ChirpForwarderConfig

	// Work chans
	incidentsChan         chan Incident
	filteredIncidentsChan chan Incident
	outChan               chan []byte
	incidentAckerChan     chan []byte

	// interfaces
	feeder        Feeder
	incidentAcker IncidentAcker
	filter        Filter
	mapper        Mapper
	forwarder     Forwarder

	// logger
	l *log.Logger
}

// NewChirpForwarder creates a new chirp forwarder
func NewChirpForwarder(cfg ChirpForwarderConfig, l *log.Logger) (cf *ChirpForwarder, err error) {
	cf = &ChirpForwarder{}

	cf.cfg = cfg

	// create work chans
	cf.incidentsChan = make(chan Incident)
	cf.filteredIncidentsChan = make(chan Incident)
	cf.outChan = make(chan []byte)
	cf.incidentAckerChan = make(chan []byte)

	// set logger
	cf.l = l
	return
}

func (cf *ChirpForwarder) setFeeder() {
	var err error
	switch cf.cfg.FeederModule {
	case "consoleapi":
		// did you specify both token file && manually using apikey+domain?
		if cf.cfg.ImConsoleTokenFile != "" && (cf.cfg.ImConsoleAPIDomain != "" || cf.cfg.ImConsoleAPIKey != "") {
			cf.l.Fatal("look, you either use 'tokenfile' or 'apikey+domain', not both")
		}
		// so, what if token file is not specfied, but neither apikey+domain?
		// we'll look for the "canarytools.config" file in user's home directory
		if cf.cfg.ImConsoleTokenFile == "" && cf.cfg.ImConsoleAPIDomain == "" && cf.cfg.ImConsoleAPIKey == "" {
			cf.l.Warn("none of 'tokenfile', 'apikey' & 'domain' has been provided! will look for 'canarytools.config' file in user's home directory")
			u, err := user.Current()
			if err != nil {
				cf.l.WithFields(log.Fields{
					"err": err,
				}).Fatal("error getting current user")
			}
			cf.cfg.ImConsoleTokenFile = path.Join(u.HomeDir, "canarytools.config")
			cf.l.WithField("path", cf.cfg.ImConsoleTokenFile).Warn("automatically looking for canarytools.config")
			if _, err := os.Stat(cf.cfg.ImConsoleTokenFile); os.IsNotExist(err) {
				cf.l.Fatal("couldn't get apikey+domain! provide using environment variables, command line flags, or path to token file")
			}
		}
		// tokenfile specified? get values from there
		if cf.cfg.ImConsoleTokenFile != "" {
			cf.cfg.ImConsoleAPIKey, cf.cfg.ImConsoleAPIDomain, err = LoadTokenFile(cf.cfg.ImConsoleTokenFile)
			if err != nil || cf.cfg.ImConsoleAPIDomain == "" || cf.cfg.ImConsoleAPIKey == "" {
				cf.l.WithFields(log.Fields{
					"err":    err,
					"api":    cf.cfg.ImConsoleAPIKey,
					"domain": cf.cfg.ImConsoleAPIDomain,
				}).Fatal("error parsing token file")
			}
			cf.l.WithFields(log.Fields{
				"path":   cf.cfg.ImConsoleTokenFile,
				"api":    cf.cfg.ImConsoleAPIKey,
				"domain": cf.cfg.ImConsoleAPIDomain,
			}).Info("successfully parsed token file, using values from there")
		}
		// few checks
		if len(cf.cfg.ImConsoleAPIKey) != 32 {
			cf.l.Fatal("invalid API Key (length != 32)")
		}
		if cf.cfg.ImConsoleAPIDomain == "" {
			cf.l.Fatal("domain must be provided")
		}
		////////////////////
		// start...
		cf.l.WithFields(log.Fields{
			"domain":                 cf.cfg.ImConsoleAPIDomain,
			"cf.cfg.ImConsoleAPIKey": (cf.cfg.ImConsoleAPIKey)[0:4] + "..." + (cf.cfg.ImConsoleAPIKey)[len(cf.cfg.ImConsoleAPIKey)-4:len(cf.cfg.ImConsoleAPIKey)],
		}).Info("ChirpForwarder Configs")

		// building a new clint, testing connection...
		cf.l.Debug("building new client and pinging console")
		c, err := NewConsoleAPIFeeder(cf.cfg.ImConsoleAPIDomain, cf.cfg.ImConsoleAPIKey, cf.cfg.ThenWhat, cf.cfg.SinceWhenString, cf.cfg.WhichIncidents, cf.cfg.ImConsoleAPIFetchInterval, cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error during creating client, or pinging console")
		}
		cf.l.Debug("ping successful! we're good to go")
		cf.feeder = c
		cf.incidentAcker = c
	default:
		cf.l.WithField("feeder", cf.cfg.FeederModule).Fatal("unsupported feeder module specified")
	}
}

func (cf *ChirpForwarder) setFilter() {
	var err error
	switch cf.cfg.IncidentFilter {
	case "none":
		cf.filter, err = NewFilterNone(cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error creating None filter")
		}
	case "dropevents":
		cf.filter, err = NewFilterDropEvents(cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error creating DropEvents filter")
		}
	default:
		cf.l.WithFields(log.Fields{
			"filter": cf.cfg.IncidentFilter,
		}).Fatal("unsupported filter")
	}
}

func (cf *ChirpForwarder) setTLSConfig() {
	var tlsConfig = &tls.Config{}
	if cf.cfg.SSLUseSSL {
		// ignore cert verification errors?
		tlsConfig.InsecureSkipVerify = cf.cfg.SSLSkipInsecure
		// custom CA?
		if cf.cfg.SSLCA != "" {
			// Get the SystemCertPool, continue with an empty pool on error
			rootCAs, _ := x509.SystemCertPool()
			if rootCAs == nil {
				rootCAs = x509.NewCertPool()
			}
			// Read in the cert file
			certs, err := ioutil.ReadFile(cf.cfg.SSLCA)
			if err != nil {
				cf.l.WithFields(log.Fields{
					"err":    err,
					"cafile": cf.cfg.SSLCA,
				}).Fatal("Failed to read CA file")
			}
			// Append our cert to the system pool
			if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
				cf.l.Fatal("couldn't add CA cert! (file might be improperly formatted)")
			}
			tlsConfig.RootCAs = rootCAs
		}
		// custom key + cert?
		if cf.cfg.SSLKey != "" && cf.cfg.SSLCert != "" {
			// Load client cert
			clientCert, err := tls.LoadX509KeyPair(cf.cfg.SSLCert, cf.cfg.SSLKey)
			if err != nil {
				cf.l.Fatal(err)
			}
			tlsConfig.Certificates = []tls.Certificate{clientCert}
		}
	}
	cf.cfg.TLSConfig = tlsConfig
}

func (cf *ChirpForwarder) setForwarder() {
	switch cf.cfg.ForwarderModule {
	case "tcp":
		// bulding new TCP out
		t, err := NewTCPForwarder(cf.cfg.OmTCPUDPHost, cf.cfg.OmTCPUDPPort, cf.cfg.TLSConfig, cf.cfg.SSLUseSSL, cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error during creating TCP Out client")
		}
		cf.forwarder = t
	case "file":
		// bulding new file out
		ff, err := NewFileForwader(cf.cfg.OmFileName, cf.cfg.OmFileMaxSize, cf.cfg.OmFileMaxBackups, cf.cfg.OmFileMaxAge, cf.cfg.OmFileCompress, cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error during creating File Out client")
		}
		cf.forwarder = ff
	case "elastic":
		// bulding new elastic out
		cfg := elasticsearch.Config{
			Addresses: []string{cf.cfg.OmElasticHost}, // A list of Elasticsearch nodes to use.
			Username:  cf.cfg.OmElasticUser,           // Username for HTTP Basic Authentication.
			Password:  cf.cfg.OmElasticPass,           // Password for HTTP Basic Authentication.
			CloudID:   cf.cfg.OmElasticCloudID,
			APIKey:    cf.cfg.OmElasticCloudAPIKey,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				ResponseHeaderTimeout: time.Duration(10) * time.Second,
				TLSClientConfig:       cf.cfg.TLSConfig,
			},
		}
		ef, err := NewElasticForwarder(cfg, cf.cfg.OmElasticIndex, cf.l)
		if err != nil {
			cf.l.WithFields(log.Fields{
				"err": err,
			}).Fatal("error during creating Elastic Out client")
		}
		cf.forwarder = ef
	case "kafka":
		// bulding new kafka out
		if cf.cfg.OmKafkaTopic == "" || cf.cfg.OmKafkaBrokers == "" {
			cf.l.Fatal("missing kafka brokers or topic")
		}
		brokers := strings.Split(cf.cfg.OmKafkaBrokers, ";")
		var kf = &KafkaForwarder{}
		if cf.cfg.SSLUseSSL {
			kf, _ = NewKafkaForwarder(brokers, cf.cfg.OmKafkaTopic, cf.cfg.TLSConfig, cf.l)
		} else {
			kf, _ = NewKafkaForwarder(brokers, cf.cfg.OmKafkaTopic, nil, cf.l)
		}
		cf.forwarder = kf
	case "":
		cf.l.Fatal("you have to provide an output module! ('-output' flag, or CANARY_OUTPUT env variable)")
	default:
		cf.l.Fatal("invalid output module specified!")
	}
}

func (cf *ChirpForwarder) setMapper() {
	var err error
	// only JSON mapper is implemented
	cf.mapper, err = NewMapperJSON(false, cf.l)
	if err != nil {
		cf.l.WithFields(log.Fields{
			"err": err,
		}).Fatal("error creating JON Mapper")
	}
}

// Run starts forwarding incidents
func (cf *ChirpForwarder) Run() {
	cf.setFeeder()
	cf.setFilter()
	cf.setTLSConfig()
	cf.setMapper()
	cf.setForwarder()

	// All good, let's roll...
	go cf.feeder.Feed(cf.incidentsChan)
	go cf.incidentAcker.AckIncidents(cf.incidentAckerChan)
	go cf.filter.Filter(cf.incidentsChan, cf.filteredIncidentsChan)
	go cf.mapper.Map(cf.filteredIncidentsChan, cf.outChan)
	cf.forwarder.Forward(cf.outChan, cf.incidentAckerChan)
}
