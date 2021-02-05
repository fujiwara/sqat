package sqat

type Message struct {
	Attributes    map[string]string
	Body          string
	MessageId     string
	ReceiptHandle string
}
