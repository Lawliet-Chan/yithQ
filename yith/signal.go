package yith

type Signal uint

const (
	NodeChange Signal = iota
	TopicChange
)

var (
	NodeChangeStr  = "node-change"
	TopicChangeStr = "topic-change"
)

var SignalTypes = []string{NodeChangeStr, TopicChangeStr}

func (st Signal) String() string {
	return SignalTypes[st]
}
