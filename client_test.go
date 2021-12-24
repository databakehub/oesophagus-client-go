package client

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestHTTP(t *testing.T) {
	address := "https://in.databake.xyz"
	c, err := InitOesophagusHTTPClient(address)
	if err != nil {
		log.Printf(err.Error())
		return
	}
	defer c.Cleanup()
	err = c.Consume(context.TODO(), "test_oeso", fmt.Sprintf("p=%d,k=%f", 133, 64.5+float64(2)), "t1=nasdaq,t2=GOOG", time.Now().UnixNano())
	if err != nil {
		log.Printf(err.Error())
		// t.Errorf("Consume() = %q, want %q", err, nil)
	} else {
		log.Printf("Consumed")
	}
}

func TestGrpc(t *testing.T) {
	address := "oesophagus-service:5000"
	c, err := InitOesophagusGrpcClient(address)
	if err != nil {
		log.Printf(err.Error())
		return
	}
	defer c.Cleanup()
	for i := 1; i <= 100; i++ {
		err := c.Consume(context.TODO(), "test_oeso", fmt.Sprintf("p=%d,l=%f", 133+i, 64.5+float64(i*2)), "t1=nasdaq,t2=GOOG", time.Now().UnixNano())
		if err != nil {
			log.Printf(err.Error())
			// t.Errorf("Consume() = %q, want %q", err, nil)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}
