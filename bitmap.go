package bm

import (
	"errors"
	"reflect"
	"sync"
	"unsafe"
)

const (
	BYTE_SIZE U64 = 8
	_0X01_    U64 = 0x01
)

var (
	ErrBucketEmpty    = errors.New("bucket is empty")
	ErrTypeNotFit     = errors.New("interface is not fit")
	ErrSizeEqualsZero = errors.New("bucket size can not be zero")
	ErrNoSuchSlot     = errors.New("no such slot for the interface")
	ErrInvalidLength  = errors.New("invalid length for the bitmapper")
)

type U64 uint64

type Interface interface {
	Len() U64
	Marshal() []byte
}

type BitMapper interface {
	Set(Interface) error
	Get(Interface) (bool, error)
	Remove(Interface) (bool, error)
	Iterator(func(U64, []byte))
}

type concurrentBitMap struct {
	bucket []byte
	len    U64
	itype  reflect.Type
	bsize  U64
	rwmut  *sync.RWMutex
}

func SizeOf(i Interface) U64 {
	return U64(reflect.TypeOf(i).Bits() / int(BYTE_SIZE))
}

func NewConcurrentBitMapper(i Interface) (BitMapper, error) {
	length := i.Len()

	if length > U64(unsafe.Sizeof(U64(0))) || length < 1 {
		return nil, ErrInvalidLength
	}

	return &concurrentBitMap{
		bucket: make([]byte, (_0X01_<<(i.Len()*BYTE_SIZE))/BYTE_SIZE),
		len:    i.Len(),
		itype:  reflect.TypeOf(i),
		bsize:  BYTE_SIZE,
		rwmut:  &sync.RWMutex{},
	}, nil
}

func (m concurrentBitMap) Set(i Interface) error {

	if err := m.checkType(i); err != nil {
		return err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return ErrNoSuchSlot
	}

	var old byte

	m.rwmut.Lock()

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		old |= (0x01 << (mod - 1))
	} else {
		j--
		old = m.bucket[j]
		old |= 0x80
	}

	m.bucket[j] = old

	m.rwmut.Unlock()

	return nil
}

func (m concurrentBitMap) Get(i Interface) (bool, error) {

	if err := m.checkType(i); err != nil {
		return false, err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return false, ErrNoSuchSlot
	}

	var old byte

	m.rwmut.RLock()
	defer m.rwmut.RUnlock()

	if len(m.bucket) == 0 {
		return false, ErrBucketEmpty
	}

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		return old&(0x01<<(mod-1)) > 0, nil
	} else {
		old = m.bucket[j-1]
		return old&(0x80) > 0, nil
	}
}

func (m concurrentBitMap) Remove(i Interface) (bool, error) {

	if err := m.checkType(i); err != nil {
		return false, err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return false, ErrNoSuchSlot
	}

	var old byte

	m.rwmut.RLock()
	defer m.rwmut.RUnlock()

	if len(m.bucket) == 0 {
		return false, ErrBucketEmpty
	}

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		m.bucket[j] &= ^(0x01 << (mod - 1))
		return old&(0x01<<(mod-1)) > 0, nil
	} else {
		old = m.bucket[j-1]
		m.bucket[j-1] &= (0x7f)
		return old&(0x80) > 0, nil
	}
}

func (m concurrentBitMap) Iterator(f func(U64, []byte)) {

	m.rwmut.RLock()
	cp := make([]byte, len(m.bucket))
	copy(cp, m.bucket)
	m.rwmut.RUnlock()

	var sum U64 = 0

	for i := 0; i < len(cp); i++ {
		_byte := cp[i]
		var j U64
		var idx U64

		if _byte == 0x00 {
			continue
		}

		for j = 1; j <= m.bsize; j++ {
			if _byte&(0x01<<(j-1)) > 0 {
				idx = U64(i)*BYTE_SIZE + j - 1
				bs := make([]byte, m.len)
				var n U64
				for n = 0; n < m.len; n++ {
					bs[n] = byte(idx >> (BYTE_SIZE * n))
				}
				f(sum, bs)
				sum++
			}
		}
	}
}

func (m concurrentBitMap) checkType(i Interface) error {
	if m.itype != reflect.TypeOf(i) {
		return ErrTypeNotFit
	}
	return nil
}

type DefaultBitMap struct {
	bucket []byte
	len    U64
	itype  reflect.Type
	bsize  U64
}

func NewDefaultBitMapper(i Interface) (BitMapper, error) {
	length := i.Len()

	if length > U64(unsafe.Sizeof(U64(0))) || length < 1 {
		return nil, ErrInvalidLength
	}

	return &DefaultBitMap{
		bucket: make([]byte, (_0X01_<<(i.Len()*BYTE_SIZE))/BYTE_SIZE),
		len:    i.Len(),
		itype:  reflect.TypeOf(i),
		bsize:  BYTE_SIZE,
	}, nil
}

func (m DefaultBitMap) Set(i Interface) error {

	if err := m.checkType(i); err != nil {
		return err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return ErrNoSuchSlot
	}

	var old byte

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		old |= (0x01 << (mod - 1))
	} else {
		j--
		old = m.bucket[j]
		old |= 0x80
	}

	m.bucket[j] = old

	return nil
}

func (m DefaultBitMap) Get(i Interface) (bool, error) {

	if err := m.checkType(i); err != nil {
		return false, err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return false, ErrNoSuchSlot
	}

	var old byte

	if len(m.bucket) == 0 {
		return false, ErrBucketEmpty
	}

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		return old&(0x01<<(mod-1)) > 0, nil
	} else {
		old = m.bucket[j-1]
		return old&(0x80) > 0, nil
	}
}

func (m DefaultBitMap) Remove(i Interface) (bool, error) {

	if err := m.checkType(i); err != nil {
		return false, err
	}

	var idx U64 = 1
	var u U64 = 0
	var bs = i.Marshal()

	for u = 0; u < U64(len(bs)); u++ {
		idx += (_0X01_ << (m.bsize * u)) * U64(bs[u])
	}

	j := idx / m.bsize

	if j > U64(len(m.bucket)) {
		return false, ErrNoSuchSlot
	}

	var old byte

	if len(m.bucket) == 0 {
		return false, ErrBucketEmpty
	}

	if mod := idx % m.bsize; mod > 0 {
		old = m.bucket[j]
		m.bucket[j] &= (^(0x01 << (mod - 1)))
		return old&(0x01<<(mod-1)) > 0, nil
	} else {
		old = m.bucket[j-1]
		m.bucket[j-1] &= 0x7f
		return old&(0x80) > 0, nil
	}
}

func (m DefaultBitMap) Iterator(f func(U64, []byte)) {

	var sum U64 = 0

	for i := 0; i < len(m.bucket); i++ {
		_byte := m.bucket[i]
		var j U64
		var idx U64

		if _byte == 0x00 {
			continue
		}

		for j = 1; j <= m.bsize; j++ {
			if _byte&(0x01<<(j-1)) > 0 {
				idx = U64(i)*BYTE_SIZE + j - 1
				bs := make([]byte, m.len)
				var n U64
				for n = 0; n < m.len; n++ {
					bs[n] = byte(idx >> (BYTE_SIZE * n))
				}
				f(sum, bs)
				sum++
			}
		}
	}
}

func (m DefaultBitMap) checkType(i Interface) error {
	if m.itype != reflect.TypeOf(i) {
		return ErrTypeNotFit
	}
	return nil
}
