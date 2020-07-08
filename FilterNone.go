package canarytools

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// FilterNone is a filter that does nothing
type FilterNone struct {
	l *log.Logger
}

func NewFilterNone(l *log.Logger) (filterNone *FilterNone, err error) {
	filterNone = &FilterNone{}
	filterNone.l = l
	return
}

// Filter filters the incidents, in this case it simply passes them through
func (fn FilterNone) Filter(incidentsChan <-chan Incident, filteredIncidentsChan chan<- Incident) {
	fn.l.WithFields(log.Fields{
		"source": "FilterNone",
		"stage":  "filter",
	}).Info("starting FilterNone")

	for v := range incidentsChan {
		fn.l.WithFields(log.Fields{
			"source":  "FilterNone",
			"stage":   "filter",
			"content": fmt.Sprintf("%#v", v),
		}).Trace("passing through value")
		fn.l.WithFields(log.Fields{
			"source":   "FilterNone",
			"stage":    "filter",
			"Incident": v.Description,
		}).Debug("Filter Incident")
		filteredIncidentsChan <- v
	}
}
