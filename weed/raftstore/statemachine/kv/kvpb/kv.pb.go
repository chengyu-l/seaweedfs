// Code generated by protoc-gen-go. DO NOT EDIT.
// source: github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/kvpb/kv.proto

package kvpb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
	raftrelaypb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Event_EventType int32

const (
	Event_PUT    Event_EventType = 0
	Event_DELETE Event_EventType = 1
)

var Event_EventType_name = map[int32]string{
	0: "PUT",
	1: "DELETE",
}

var Event_EventType_value = map[string]int32{
	"PUT":    0,
	"DELETE": 1,
}

func (x Event_EventType) String() string {
	return proto.EnumName(Event_EventType_name, int32(x))
}

func (Event_EventType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_fdaf0ce0883046fb, []int{0, 0}
}

type Event struct {
	// type is the kind of event. If type is a PUT, it indicates
	// new data has been stored to the key. If type is a DELETE,
	// it indicates the key was deleted.
	Type Event_EventType `protobuf:"varint,1,opt,name=type,proto3,enum=kvpb.Event_EventType" json:"type,omitempty"`
	// kv holds the KeyValue for the event.
	// A PUT event contains current kv pair.
	// A PUT event with kv.Version=1 indicates the creation of a key.
	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.
	Kv                   *raftrelaypb.KeyValue `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_fdaf0ce0883046fb, []int{0}
}

func (m *Event) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event.Unmarshal(m, b)
}
func (m *Event) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event.Marshal(b, m, deterministic)
}
func (m *Event) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event.Merge(m, src)
}
func (m *Event) XXX_Size() int {
	return xxx_messageInfo_Event.Size(m)
}
func (m *Event) XXX_DiscardUnknown() {
	xxx_messageInfo_Event.DiscardUnknown(m)
}

var xxx_messageInfo_Event proto.InternalMessageInfo

func (m *Event) GetType() Event_EventType {
	if m != nil {
		return m.Type
	}
	return Event_PUT
}

func (m *Event) GetKv() *raftrelaypb.KeyValue {
	if m != nil {
		return m.Kv
	}
	return nil
}

func init() {
	proto.RegisterEnum("kvpb.Event_EventType", Event_EventType_name, Event_EventType_value)
	proto.RegisterType((*Event)(nil), "kvpb.Event")
}

func init() {
	proto.RegisterFile("github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/kvpb/kv.proto", fileDescriptor_fdaf0ce0883046fb)
}

var fileDescriptor_fdaf0ce0883046fb = []byte{
	// 205 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xb2, 0xcc, 0x4b, 0x4d, 0x2a,
	0xcd, 0x49, 0x2c, 0xd6, 0x87, 0xd2, 0x69, 0xc5, 0xfa, 0x45, 0x89, 0x69, 0x25, 0xc5, 0x25, 0xf9,
	0x45, 0xa9, 0xfa, 0xc5, 0x25, 0x89, 0x25, 0xa9, 0xb9, 0x89, 0xc9, 0x19, 0x99, 0x79, 0xa9, 0xfa,
	0xd9, 0x65, 0xfa, 0xd9, 0x65, 0x05, 0x49, 0xfa, 0xd9, 0x65, 0x7a, 0x05, 0x45, 0xf9, 0x25, 0xf9,
	0x42, 0x2c, 0x20, 0xae, 0x94, 0x13, 0x3e, 0x03, 0x40, 0xac, 0xa2, 0xd4, 0x9c, 0xc4, 0x4a, 0x04,
	0xab, 0x20, 0x09, 0xcc, 0x8e, 0x07, 0x73, 0x20, 0x26, 0x29, 0x55, 0x73, 0xb1, 0xba, 0x96, 0xa5,
	0xe6, 0x95, 0x08, 0x69, 0x72, 0xb1, 0x94, 0x54, 0x16, 0xa4, 0x4a, 0x30, 0x2a, 0x30, 0x6a, 0xf0,
	0x19, 0x89, 0xea, 0x81, 0x6c, 0xd0, 0x03, 0x4b, 0x41, 0xc8, 0x90, 0xca, 0x82, 0xd4, 0x20, 0xb0,
	0x12, 0x21, 0x55, 0x2e, 0xa6, 0xec, 0x32, 0x09, 0x26, 0x05, 0x46, 0x0d, 0x6e, 0x23, 0x51, 0x3d,
	0x24, 0xe3, 0xf5, 0xbc, 0x53, 0x2b, 0xc3, 0x12, 0x73, 0x4a, 0x53, 0x83, 0x98, 0xb2, 0xcb, 0x94,
	0x14, 0xb8, 0x38, 0xe1, 0x3a, 0x85, 0xd8, 0xb9, 0x98, 0x03, 0x42, 0x43, 0x04, 0x18, 0x84, 0xb8,
	0xb8, 0xd8, 0x5c, 0x5c, 0x7d, 0x5c, 0x43, 0x5c, 0x05, 0x18, 0x93, 0xd8, 0xc0, 0x6e, 0x30, 0x06,
	0x04, 0x00, 0x00, 0xff, 0xff, 0x77, 0xde, 0x97, 0x08, 0x0a, 0x01, 0x00, 0x00,
}