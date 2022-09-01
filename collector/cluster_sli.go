package collector

import (
	"bytes"
	"encoding/json"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus-community/elasticsearch_exporter/pkg/clusterinfo"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"
)

var (
	defaultClusterSliLabels = []string{"cluster"}
)

type monitorIndex struct {
	Settings struct {
		NumberOfShards                        int    `json:"number_of_shards"`
		NumberOfReplicas                      int    `json:"number_of_replicas"`
		IndexUnassignedNodeLeftDelayedTimeout string `json:"index.unassigned.node_left.delayed_timeout"`
	} `json:"settings"`
	Mappings struct {
		Dynamic    bool `json:"dynamic"`
		Properties struct {
			MonitorDurability struct {
				Type string `json:"type"`
			} `json:"monitor_durability"`
		} `json:"properties"`
	} `json:"mappings"`
}

type bulkIndex struct {
	Index struct {
		Id      int `json:"_id"`
		Routing int `json:"routing"`
	} `json:"index"`
}
type bulkDoc struct {
	Doc struct {
		MonitorDurability int64 `json:"monitor_durability"`
	} `json:"doc"`
}

type shardRouting struct {
	Id      int `json:"_id"`
	Routing int `json:"routing"`
}

type ClusterLabels struct {
	keys   func() string
	values func(*clusterinfo.Response) string
}

type MgetResult struct {
	totalRequest   int64
	totalSuccess   int64
	successPersist int64
	searchLantency int64
}

type BulkResult struct {
	totalInsertRequest int64
	totalSuccessInsert int64
	insertLantency     int64
}

type sliAvailabilityMetric struct {
	Type          prometheus.ValueType
	Desc          *prometheus.Desc
	Value         func(data MgetResult) float64
	ClusterLabels ClusterLabels
}

type sliDurabilityMetrics struct {
	Type          prometheus.ValueType
	Desc          *prometheus.Desc
	Value         func(data BulkResult) float64
	ClusterLabels ClusterLabels
}

type sliLatencyMetric struct {
	Type          prometheus.ValueType
	Desc          *prometheus.Desc
	Value         func(searchLantency int64, insertLantency int64) float64
	ClusterLabels ClusterLabels
}

type ClusterSli struct {
	logger log.Logger
	client *http.Client
	url    *url.URL

	clusterInfoCh   chan *clusterinfo.Response
	lastClusterInfo *clusterinfo.Response
	lastInsertValue int64

	up                prometheus.Gauge
	totalScrapes      prometheus.Counter
	jsonParseFailures prometheus.Counter

	sliAvailabilityMetrics []*sliAvailabilityMetric
	sliLatencyMetrics      []*sliLatencyMetric
	sliDurabilityMetrics   []*sliDurabilityMetrics
}

func NewClusterSli(logger log.Logger, client *http.Client, url *url.URL) *ClusterSli {
	subsystem := "cluster_sli"

	clusterLabels := ClusterLabels{
		keys: func() string {
			return "cluster"
		},
		values: func(lastClusterinfo *clusterinfo.Response) string {
			if lastClusterinfo != nil {
				return lastClusterinfo.ClusterName
			}
			// this shouldn't happen, as the clusterinfo Retriever has a blocking
			// Run method. It blocks until the first clusterinfo call has succeeded
			return "unknown_cluster"
		},
	}

	clusterSli := &ClusterSli{
		logger: logger,
		client: client,
		url:    url,

		clusterInfoCh: make(chan *clusterinfo.Response),
		lastClusterInfo: &clusterinfo.Response{
			ClusterName: "unknown_cluster",
		},
		lastInsertValue: int64(0),

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(namespace, subsystem, "up"),
			Help: "Was the last scrape of the ElasticSearch cluster health endpoint successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, subsystem, "total_scrapes"),
			Help: "Current total ElasticSearch cluster health scrapes.",
		}),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, subsystem, "json_parse_failures"),
			Help: "Number of errors while parsing JSON.",
		}),
		sliAvailabilityMetrics: []*sliAvailabilityMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "search_request_batch_num"),
					"The number of shards mget search. If request status not 200 will be 1,means failed request",
					defaultClusterSliLabels, nil,
				),
				Value: func(mgetResult MgetResult) float64 {
					return float64(mgetResult.totalRequest)
				},
				ClusterLabels: clusterLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "search_request_success_num"),
					"The number of shards mget search success get to shards. If request status not 200 will be 0,means failed request",
					defaultClusterSliLabels, nil,
				),
				Value: func(mgetResult MgetResult) float64 {
					return float64(mgetResult.totalSuccess)
				},
				ClusterLabels: clusterLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "search_confirm_persist_last_insert"),
					"Use last insert value to confirm durability",
					defaultClusterSliLabels, nil,
				),
				Value: func(mgetResult MgetResult) float64 {
					return float64(mgetResult.successPersist)
				},
				ClusterLabels: clusterLabels,
			},
		},
		sliLatencyMetrics: []*sliLatencyMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "search_request_batch_lantency"),
					"The latency of mget for monitor-sli shards",
					defaultClusterSliLabels, nil,
				),
				Value: func(searchLantency int64, insertLantency int64) float64 {
					return float64(searchLantency)
				},
				ClusterLabels: clusterLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "insert_request_batch_lantency"),
					"The latency of bulk for monitor-sli shards,elastisearch timeout is 2s,http timeout is 5s",
					defaultClusterSliLabels, nil,
				),
				Value: func(searchLantency int64, insertLantency int64) float64 {
					return float64(insertLantency)
				},
				ClusterLabels: clusterLabels,
			},
		},
		sliDurabilityMetrics: []*sliDurabilityMetrics{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "insert_request_batch_num"),
					"The number of shards bulk insert. If request status not 200 will be 1,means failed request",
					defaultClusterSliLabels, nil,
				),
				Value: func(bulkResult BulkResult) float64 {
					return float64(bulkResult.totalInsertRequest)
				},
				ClusterLabels: clusterLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, subsystem, "bulk_success_persist_num"),
					"The number of shards bulk search insert. If request status not 200 will be 30 ,because it worked last time by default.",
					defaultClusterSliLabels, nil,
				),
				Value: func(bulkResult BulkResult) float64 {
					return float64(bulkResult.totalSuccessInsert)
				},
				ClusterLabels: clusterLabels,
			},
		},
	}

	// start go routine to fetch clusterinfo updates and save them to lastClusterinfo
	go func() {
		_ = level.Debug(logger).Log("msg", "detect if .monitor-sli exist")
		clusterSli.createMonitorIndexIfNotExist()
		_ = level.Debug(logger).Log("msg", "starting insert first bulk")
		clusterSli.bulkAndCalculateSli()
		_ = level.Debug(logger).Log("msg", "starting cluster info receive loop")
		for ci := range clusterSli.clusterInfoCh {
			if ci != nil {
				_ = level.Debug(logger).Log("msg", "received cluster info update", "cluster", ci.ClusterName)
				clusterSli.lastClusterInfo = ci
			}
		}
		_ = level.Debug(logger).Log("msg", "exiting cluster info receive loop")
	}()

	return clusterSli

}

// ClusterLabelUpdates returns a pointer to a channel to receive cluster info updates. It implements the
// (not exported) clusterinfo.consumer interface
func (c *ClusterSli) ClusterLabelUpdates() *chan *clusterinfo.Response {
	return &c.clusterInfoCh
}

// String implements the stringer interface. It is part of the clusterinfo.consumer interface
func (c *ClusterSli) String() string {
	return namespace + "sli"
}

// Describe set Prometheus metrics descriptions.
func (c *ClusterSli) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.sliAvailabilityMetrics {
		ch <- metric.Desc
	}

	for _, sliLatencyMetric := range c.sliLatencyMetrics {
		ch <- sliLatencyMetric.Desc
	}

	for _, sliDurabilityMetric := range c.sliDurabilityMetrics {
		ch <- sliDurabilityMetric.Desc
	}

	ch <- c.up.Desc()
	ch <- c.totalScrapes.Desc()
	ch <- c.jsonParseFailures.Desc()
}

func (c *ClusterSli) fetchAndCalculateSli() MgetResult {
	var mr mgetResponse
	u := *c.url
	u.Path = path.Join("/.monitoring-sli/_doc/_mget")

	shardRoutingList := [30]shardRouting{}
	for searchShardValue := int(0); searchShardValue < 30; searchShardValue++ {
		shardRoutingList[searchShardValue] = shardRouting{searchShardValue, searchShardValue}
	}

	searchbody := map[string][30]shardRouting{
		"docs": shardRoutingList,
	}

	bytesData, _ := json.Marshal(searchbody)

	reader := bytes.NewReader(bytesData)

	start := time.Now().UnixMilli()

	mgetRes, err := c.client.Post(u.String(), "application/json", reader)

	end := time.Now().UnixMilli()
	elapsed := end - start

	if err != nil {
		_ = level.Warn(c.logger).Log(
			"msg", "failed to connect http",
			"err", err,
		)
		return MgetResult{1, 0, 0, elapsed}
	}

	defer func() {
		err = mgetRes.Body.Close()
		if err != nil {
			_ = level.Warn(c.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if mgetRes.StatusCode != http.StatusOK {
		return MgetResult{1, 0, 0, elapsed}
	}

	bts, err := ioutil.ReadAll(mgetRes.Body)
	if err != nil {
		c.jsonParseFailures.Inc()
		return MgetResult{1, 0, 0, elapsed}
	}

	if err := json.Unmarshal(bts, &mr); err != nil {
		c.jsonParseFailures.Inc()
		return MgetResult{1, 0, 0, elapsed}
	}

	totalShard := int64(0)
	successSearch := int64(0)
	successPersist := int64(0)
	for _, oneSearchResult := range mr.Docs {
		totalShard++
		if oneSearchResult.Found == true {
			successSearch++
			if int64(oneSearchResult.Source.Doc.MonitorDurability) == c.lastInsertValue {
				successPersist++
			}
		}

	}

	return MgetResult{totalShard, successSearch, successPersist, elapsed}

}
func (c *ClusterSli) bulkAndCalculateSli() BulkResult {
	var br bulkResponse
	u := *c.url
	u.RawQuery = "timeout=2s"
	u.Path = path.Join("/.monitoring-sli/_bulk")

	var ndJsonString string = ""
	var timeBulkValue int64 = time.Now().UnixMilli()

	for shardsValue := 0; shardsValue < 30; shardsValue++ {
		bulkIndex := bulkIndex{}
		bulkIndex.Index.Id = shardsValue
		bulkIndex.Index.Routing = shardsValue

		indexData, _ := json.Marshal(bulkIndex)
		ndJsonString = ndJsonString + string(indexData) + "\n"

		bulkDoc := bulkDoc{}
		bulkDoc.Doc.MonitorDurability = timeBulkValue

		docData, _ := json.Marshal(bulkDoc)
		ndJsonString = ndJsonString + string(docData) + "\n"

	}

	reader := bytes.NewReader([]byte(ndJsonString))

	start := time.Now().UnixMilli()

	req, _ := http.NewRequest("PUT", u.String(), reader)
	req.Header.Set("Content-Type", "application/json")
	bulkRes, err := c.client.Do(req)
	end := time.Now().UnixMilli()
	elapsed := end - start

	if err != nil {
		_ = level.Warn(c.logger).Log(
			"msg", "failed to connect elasticserch ",
			"err", err,
		)
		return BulkResult{1, 0, elapsed}
	}

	defer func() {
		err = bulkRes.Body.Close()
		if err != nil {
			_ = level.Warn(c.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if bulkRes.StatusCode != http.StatusOK {
		return BulkResult{1, 0, elapsed}
	}

	bts, err := ioutil.ReadAll(bulkRes.Body)
	if err != nil {
		c.jsonParseFailures.Inc()
		return BulkResult{1, 0, elapsed}
	}

	if err := json.Unmarshal(bts, &br); err != nil {
		c.jsonParseFailures.Inc()
		return BulkResult{1, 0, elapsed}
	}

	totalShard := int64(0)
	successInsert := int64(0)

	for _, item := range br.Items {
		totalShard++
		if item.Index.Status == http.StatusOK {
			successInsert++
		}

	}
	c.lastInsertValue = timeBulkValue

	return BulkResult{totalShard, successInsert, elapsed}

}

func (c *ClusterSli) createMonitorIndexIfNotExist() {
	u := *c.url
	u.Path = path.Join("/.monitoring-sli")
	res, err := c.client.Get(u.String())
	if err != nil {
		_ = level.Warn(c.logger).Log(
			"msg", "failed to detect .monitor-sli ,http.Client not connect",
			"err", err,
		)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(c.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode == http.StatusNotFound {
		monitorIndexSettings := monitorIndex{}
		monitorIndexSettings.Settings.NumberOfShards = 30
		monitorIndexSettings.Settings.NumberOfReplicas = 1
		monitorIndexSettings.Settings.IndexUnassignedNodeLeftDelayedTimeout = "10m"
		monitorIndexSettings.Mappings.Dynamic = false
		monitorIndexSettings.Mappings.Properties.MonitorDurability.Type = "long"

		bytesData, _ := json.Marshal(monitorIndexSettings)

		reader := bytes.NewReader(bytesData)
		req, _ := http.NewRequest("PUT", u.String(), reader)
		req.Header.Set("Content-Type", "application/json")
		c.client.Do(req)

	}

}

// Collect collects sli metrics.
func (c *ClusterSli) Collect(ch chan<- prometheus.Metric) {
	c.totalScrapes.Inc()
	defer func() {
		ch <- c.up
		ch <- c.totalScrapes
		ch <- c.jsonParseFailures
	}()

	mgetResult := c.fetchAndCalculateSli()

	bulkResult := c.bulkAndCalculateSli()

	c.up.Set(1)

	for _, metric := range c.sliAvailabilityMetrics {
		ch <- prometheus.MustNewConstMetric(
			metric.Desc,
			metric.Type,
			metric.Value(mgetResult),
			metric.ClusterLabels.values(c.lastClusterInfo),
		)
	}

	for _, sliLatencyMetric := range c.sliLatencyMetrics {
		ch <- prometheus.MustNewConstMetric(
			sliLatencyMetric.Desc,
			sliLatencyMetric.Type,
			sliLatencyMetric.Value(mgetResult.searchLantency, bulkResult.insertLantency),
			sliLatencyMetric.ClusterLabels.values(c.lastClusterInfo),
		)
	}

	for _, sliDurabilityMetric := range c.sliDurabilityMetrics {
		ch <- prometheus.MustNewConstMetric(
			sliDurabilityMetric.Desc,
			sliDurabilityMetric.Type,
			sliDurabilityMetric.Value(bulkResult),
			sliDurabilityMetric.ClusterLabels.values(c.lastClusterInfo),
		)
	}

}
