package collector

type bulkResponse struct {
	Took   int                `json:"took"`
	Errors bool               `json:"errors"`
	Items  []oneShardResponse `json:"items"`
}
type oneShardResponse struct {
	Index struct {
		Index   string `json:"_index"`
		Type    string `json:"_type"`
		Id      string `json:"_id"`
		Version int    `json:"_version"`
		Result  string `json:"result"`
		Shards  struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Failed     int `json:"failed"`
		} `json:"_shards"`
		SeqNo       int `json:"_seq_no"`
		PrimaryTerm int `json:"_primary_term"`
		Status      int `json:"status"`
		Error       struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	} `json:"index"`
}
