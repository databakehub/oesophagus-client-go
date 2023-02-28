package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	pb "github.com/databakehub/oesophagus-client-go/oesophagus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PublicAddress is the public IP address of oesophagus server
const PublicAddress = "https://in.databake.xyz"

// OesophagusClient is the client data structure that is exported
type OesophagusClient struct {
	grpcConn   *grpc.ClientConn
	grpcClient pb.OesophagusServiceClient
	address    string
	isGrpc     bool
}

// ConsumeGrpc sends message via GRPC
func (x *OesophagusClient) ConsumeGrpc(ctx context.Context, message *pb.Swallow) (*pb.Burp, error) {
	return x.grpcClient.Consume(ctx, message)
}

// ConsumeHTTP sends message via HTTP POST
func (x *OesophagusClient) ConsumeHTTP(ctx context.Context, message *pb.Swallow) (*pb.Burp, error) {
	jsonStr, _ := json.Marshal(message)
	req, _ := http.NewRequest("POST", x.address, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	ret := &pb.Burp{}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ret, err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, ret)
	if ret.Status != 200 && err == nil {
		err = errors.New("oesophagus error")
	}
	return ret, err
}

// Consume consumes the message and forwards it to the correct client
func (x *OesophagusClient) Consume(
	ctx context.Context,
	measurement string,
	fieldSet string,
	tagSet string,
	timestamp int64) error {
	var err error
	if x.isGrpc {
		_, err = x.ConsumeGrpc(ctx, &pb.Swallow{Measurement: measurement, FieldSet: fieldSet, TagSet: tagSet, Timestamp: timestamp})
	} else {
		_, err = x.ConsumeHTTP(ctx, &pb.Swallow{Measurement: measurement, FieldSet: fieldSet, TagSet: tagSet, Timestamp: timestamp})
	}
	if err != nil {
		log.Println("Swallow error:", err.Error())
	}
	return err
}

// Cleanup cleans up open connections
func (x *OesophagusClient) Cleanup() {
	if x.grpcConn != nil {
		x.grpcConn.Close()
	}
}

// InitOesophagusGrpcClient initializes grpc client
func InitOesophagusGrpcClient(address string) (*OesophagusClient, error) {
	creds := insecure.NewCredentials()
	conn, err := grpc.Dial(address,
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	client := pb.NewOesophagusServiceClient(conn)
	return &OesophagusClient{conn, client, address, true}, nil
}

// InitOesophagusHTTPClient initializes Http client
func InitOesophagusHTTPClient(address string) (*OesophagusClient, error) {
	return &OesophagusClient{nil, nil, address, false}, nil
}
