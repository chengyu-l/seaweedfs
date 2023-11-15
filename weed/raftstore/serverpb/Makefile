all: gen

.PHONY : gen

gen:
	protoc3 server_pb.proto --go_out=plugins=grpc:.
	protoc3 internal.proto --go_out=plugins=grpc:.
	protoc3 internal.proto --grpc-gateway_out=logtostderr=true:.
