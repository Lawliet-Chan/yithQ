package message

type Message struct {
	ID         int64  `json:"id"`
	Body       []byte `json:"body"`
	Timestamp  int64  `json:"timestamp"`
	ProducerIP string `json:"producer_ip"`
	SeqNum     uint64 `json:"seq_num"`
}

type Messages struct {
	Topic       string     `json:"topic"`
	Msgs        []*Message `json:"msgs"`
	MetaVersion uint32     `json:"meta_version"`
}
