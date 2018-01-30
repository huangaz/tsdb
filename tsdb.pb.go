// Code generated by protoc-gen-go. DO NOT EDIT.
// source: lib/tsdb/tsdb.proto

/*
Package tsdb is a generated protocol buffer package.

It is generated from these files:
	lib/tsdb/tsdb.proto

It has these top-level messages:
	Key
	TimeValuePair
	DataPoint
	DataPoints
*/
package tsdb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Key struct {
	Key     []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ShardId int32  `protobuf:"varint,2,opt,name=shardId" json:"shardId,omitempty"`
}

func (m *Key) Reset()                    { *m = Key{} }
func (m *Key) String() string            { return proto.CompactTextString(m) }
func (*Key) ProtoMessage()               {}
func (*Key) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Key) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *Key) GetShardId() int32 {
	if m != nil {
		return m.ShardId
	}
	return 0
}

type TimeValuePair struct {
	Timestamp int64   `protobuf:"varint,1,opt,name=timestamp" json:"timestamp,omitempty"`
	Value     float64 `protobuf:"fixed64,2,opt,name=value" json:"value,omitempty"`
}

func (m *TimeValuePair) Reset()                    { *m = TimeValuePair{} }
func (m *TimeValuePair) String() string            { return proto.CompactTextString(m) }
func (*TimeValuePair) ProtoMessage()               {}
func (*TimeValuePair) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *TimeValuePair) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *TimeValuePair) GetValue() float64 {
	if m != nil {
		return m.Value
	}
	return 0
}

type DataPoint struct {
	Key   *Key           `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Value *TimeValuePair `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
}

func (m *DataPoint) Reset()                    { *m = DataPoint{} }
func (m *DataPoint) String() string            { return proto.CompactTextString(m) }
func (*DataPoint) ProtoMessage()               {}
func (*DataPoint) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *DataPoint) GetKey() *Key {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *DataPoint) GetValue() *TimeValuePair {
	if m != nil {
		return m.Value
	}
	return nil
}

type DataPoints struct {
	Key    *Key             `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Values []*TimeValuePair `protobuf:"bytes,2,rep,name=values" json:"values,omitempty"`
}

func (m *DataPoints) Reset()                    { *m = DataPoints{} }
func (m *DataPoints) String() string            { return proto.CompactTextString(m) }
func (*DataPoints) ProtoMessage()               {}
func (*DataPoints) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *DataPoints) GetKey() *Key {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *DataPoints) GetValues() []*TimeValuePair {
	if m != nil {
		return m.Values
	}
	return nil
}

func init() {
	proto.RegisterType((*Key)(nil), "tsdb.Key")
	proto.RegisterType((*TimeValuePair)(nil), "tsdb.TimeValuePair")
	proto.RegisterType((*DataPoint)(nil), "tsdb.DataPoint")
	proto.RegisterType((*DataPoints)(nil), "tsdb.DataPoints")
}

func init() { proto.RegisterFile("lib/tsdb/tsdb.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 235 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0x12, 0xce, 0xc9, 0x4c, 0xd2,
	0x2f, 0x29, 0x4e, 0x81, 0x10, 0x7a, 0x05, 0x45, 0xf9, 0x25, 0xf9, 0x42, 0x2c, 0x20, 0xb6, 0x92,
	0x21, 0x17, 0xb3, 0x77, 0x6a, 0xa5, 0x90, 0x00, 0x17, 0x73, 0x76, 0x6a, 0xa5, 0x04, 0xa3, 0x02,
	0xa3, 0x06, 0x4f, 0x10, 0x88, 0x29, 0x24, 0xc1, 0xc5, 0x5e, 0x9c, 0x91, 0x58, 0x94, 0xe2, 0x99,
	0x22, 0xc1, 0xa4, 0xc0, 0xa8, 0xc1, 0x1a, 0x04, 0xe3, 0x2a, 0x39, 0x73, 0xf1, 0x86, 0x64, 0xe6,
	0xa6, 0x86, 0x25, 0xe6, 0x94, 0xa6, 0x06, 0x24, 0x66, 0x16, 0x09, 0xc9, 0x70, 0x71, 0x96, 0x64,
	0xe6, 0xa6, 0x16, 0x97, 0x24, 0xe6, 0x16, 0x80, 0x8d, 0x60, 0x0e, 0x42, 0x08, 0x08, 0x89, 0x70,
	0xb1, 0x96, 0x81, 0x94, 0x82, 0x8d, 0x61, 0x0c, 0x82, 0x70, 0x94, 0x82, 0xb9, 0x38, 0x5d, 0x12,
	0x4b, 0x12, 0x03, 0xf2, 0x33, 0xf3, 0x4a, 0x84, 0xa4, 0x11, 0xb6, 0x73, 0x1b, 0x71, 0xea, 0x81,
	0x1d, 0xe9, 0x9d, 0x5a, 0x09, 0x71, 0x88, 0x26, 0xb2, 0x7e, 0x6e, 0x23, 0x61, 0x88, 0x34, 0x8a,
	0x0b, 0x60, 0x86, 0x86, 0x71, 0x71, 0xc1, 0x0d, 0x2d, 0xc6, 0x6f, 0xaa, 0x36, 0x17, 0x1b, 0x58,
	0x4f, 0xb1, 0x04, 0x93, 0x02, 0x33, 0x2e, 0x63, 0xa1, 0x4a, 0x9c, 0x14, 0xa3, 0xe4, 0xd3, 0x33,
	0x4b, 0x32, 0x4a, 0x93, 0xf4, 0x92, 0xf3, 0x73, 0xf5, 0x2b, 0x4b, 0x93, 0xf2, 0xf5, 0xd3, 0x12,
	0x73, 0x92, 0xf3, 0xf3, 0xf4, 0x61, 0x01, 0x9b, 0xc4, 0x06, 0x0e, 0x54, 0x63, 0x40, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x20, 0x56, 0x17, 0xae, 0x6b, 0x01, 0x00, 0x00,
}