package collector

type mgetResponse struct {
	Docs []oneDocResponse `json:"docs"`
}

type oneDocResponse struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Routing     string `json:"_routing"`
	Found       bool   `json:"found"`
	Source      struct {
		Doc struct {
			MonitorDurability int `json:"monitor_durability"`
		} `json:"doc"`
	} `json:"_source"`
}
