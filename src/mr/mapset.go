package mr

type MapSet struct {
	mapbool map[interface{}]bool
	count   int
}

func NewMapSet() *MapSet {
	// return a new MapSet
	return &MapSet{
		mapbool: make(map[interface{}]bool),
		count:   0,
	}
}

func (ma *MapSet) Insert(data interface{}) {
	// insert a data into the set
	ma.mapbool[data] = true
	ma.count++
}

func (ma *MapSet) Delete(data interface{}) {
	// delete a data from the set
	ma.mapbool[data] = false
	ma.count--
}

func (ma *MapSet) Contains(data interface{}) bool {
	// return true if the set contains data
	return ma.mapbool[data]
}

func (ma *MapSet) Size() int {
	// return the number of elements in the set
	return ma.count
}
