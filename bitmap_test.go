package bm

import (
	"testing"
	"time"
)

type u8 uint8

func (m u8) Len() U64 {
	return 1
}
func (m u8) Marshal() []byte {
	return []byte{
		0: byte(m),
	}
}

type u16 uint16

func (m u16) Len() U64 {
	return 2
}
func (m u16) Marshal() []byte {
	return []byte{
		0: byte(m),
		1: byte(m >> 8),
	}
}

func shocks(t *testing.T) func(error) {
	return func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func prints(t *testing.T) func(...interface{}) {
	return func(i ...interface{}) {
		t.Log(i...)
	}
}

func TestConcurrent(t *testing.T) {

	shock := shocks(t)
	print := prints(t)

	var u u16
	bm, err := NewConcurrentBitMapper(u)

	shock(err)

	var loop = func() {
		for i := 0; i < 0xff; i++ {
			bm.Set(u8(i))
		}
	}

	for i := 0; i < 0xff_ff; i++ {
		bm.Set(u16(i))
	}

	go loop()
	go loop()
	go loop()

	time.Sleep(1 * time.Second)

	bm.Iterator(func(u U64, b []byte) {
		t := u16(b[0])
		t += (u16(b[1]) << 8)

		if u == U64(t) {
			print(u, t)
		}
	})

}

func TestDefault(t *testing.T) {

	shock := shocks(t)
	print := prints(t)

	var u u8
	bm, err := NewDefaultBitMapper(u)

	shock(err)

	bm.Set(u8(0))
	bm.Set(u8(1))
	bm.Set(u8(2))
	bm.Set(u8(3))
	bm.Set(u8(4))
	bm.Set(u8(5))
	bm.Set(u8(8))
	bm.Set(u8(12))

	bm.Remove(u8(8))

	for i := 0; i < 0xff_ff; i++ {
		bm.Set(u8(i))
	}

	for i := 0; i < 0xff; i++ {
		print(bm.Get(u8(i)))
	}

	var get = func(i Interface) bool {
		b, _ := bm.Get(i)
		return b
	}

	print("0=", get(u8(0)))
	print("1=", get(u8(1)))
	print("2=", get(u8(2)))
	print("3=", get(u8(3)))
	print("4=", get(u8(4)))
	print("5=", get(u8(5)))
	print("6=", get(u8(6)))
	print("7=", get(u8(7)))
	print("8=", get(u8(8)))
	print("9=", get(u8(9)))
	print("12=", get(u8(12)))

	time.Sleep(1 * time.Second)

	bm.Iterator(func(u U64, b []byte) {
		t := u8(b[0])
		if u != U64(t) {
			print(u, t)
		}
	})

}
