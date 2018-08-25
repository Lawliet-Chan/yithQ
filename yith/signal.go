package yith

type Signal struct {
	Typ SignalType `json:"typ"`
}

type SignalType uint

const (
	NodeChange SignalType = iota
	TopicChange
)

var (
	NodeChangeStr  = "node-change"
	TopicChangeStr = "topic-change"
)

var SignalTypes = []string{NodeChangeStr, TopicChangeStr}

func (st SignalType) String() string {
	return SignalTypes[st]
}
