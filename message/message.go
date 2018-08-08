package message

type Message struct {
	ID         int64
	Body       interface{}
	Timestamp  int64
	ProducerIP string
	SeqNum     uint64
}

func ToBytes(msg *Message) ([]byte, error) {

}

func ToMessage(data []byte) (*Message, error) {

}
