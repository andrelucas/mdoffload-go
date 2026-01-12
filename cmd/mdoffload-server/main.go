package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	mdoffloadv1 "github.com/andrelucas/mdoffload-go/bits.linode.com/LinodeApi/obj-endpoint/gen/proto/mdoffload/v1"
	"github.com/andrelucas/mdoffload-go/internal/service"
	"github.com/andrelucas/mdoffload-go/pkg/storage"
	"google.golang.org/grpc"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:8004", "gRPC listen address")
	flag.Parse()

	store := storage.NewInMemoryStore()
	grpcServer := grpc.NewServer()
	mdoffloadv1.RegisterMDOffloadServiceServer(grpcServer, service.NewMDOffloadServer(store))

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddr, err)
	}

	log.Printf("mdoffload server listening on %s", *listenAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Printf("shutdown signal received, stopping gRPC server")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
