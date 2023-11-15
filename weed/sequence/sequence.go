package sequence

type Sequencer interface {
	NextFileId(count uint64) uint64
	SetMax(uint64)
	Peek() uint64
	Initialize(c, m uint64)
}
