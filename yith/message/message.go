package message

type Message struct {
	ID        int64
	Body      []byte
	Timestamp int64
}

func ToBytes(msg *Message) ([]byte, error) {

}

func ToMessage(data []byte) (*Message, error) {

}
