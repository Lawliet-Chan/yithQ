package meta

type Signal uint

const (
	NodeChange Signal = iota
	TopicAddChange
	TopicDeleteChange
)

var (
	NodeChangeStr        = "node-change"
	TopicAddChangeStr    = "topic-add-change"
	TopicDeleteChangeStr = "topic-delete-change"
)

var SignalTypes = []string{NodeChangeStr, TopicAddChangeStr, TopicDeleteChangeStr}

func (st Signal) String() string {
	return SignalTypes[st]
}
