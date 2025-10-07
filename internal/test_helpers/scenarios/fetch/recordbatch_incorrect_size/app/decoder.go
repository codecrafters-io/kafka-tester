package main

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Decoder provides methods for decoding Kafka protocol data
type Decoder interface {
	ReadUint16() (uint16, error)
	ReadUint32() (uint32, error)
	ReadUint64() (uint64, error)
	ReadBytes(length int) ([]byte, error)
	ReadString() (string, error)
	ReadUVarint() (uint64, error)
	ReadCompactArrayHeader() (int, error)
}

// SimpleDecoder implements basic Kafka decoding
type SimpleDecoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader *bufio.Reader) *SimpleDecoder {
	return &SimpleDecoder{reader: reader}
}

func (d *SimpleDecoder) ReadUint16() (uint16, error) {
	var value uint16
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *SimpleDecoder) ReadUint32() (uint32, error) {
	var value uint32
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *SimpleDecoder) ReadUint64() (uint64, error) {
	var value uint64
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *SimpleDecoder) ReadBytes(length int) ([]byte, error) {
	data := make([]byte, length)
	_, err := io.ReadFull(d.reader, data)
	return data, err
}

func (d *SimpleDecoder) ReadString() (string, error) {
	length, err := d.ReadUint16()
	if err != nil {
		return "", err
	}
	if length == 0xFFFF { // -1 as uint16 means null
		return "", nil
	}
	data, err := d.ReadBytes(int(length))
	return string(data), err
}

func (d *SimpleDecoder) ReadUVarint() (uint64, error) {
	var result uint64
	var shift uint
	for {
		b, err := d.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return result, nil
}

func (d *SimpleDecoder) ReadCompactArrayHeader() (int, error) {
	length, err := d.ReadUVarint()
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return -1, nil // null array
	}
	return int(length - 1), nil
}
