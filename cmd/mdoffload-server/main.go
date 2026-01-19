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
	backend := flag.String("backend", "tikv", "storage backend: tikv|memory")
	verbose := flag.Bool("verbose", false, "log incoming requests and outgoing responses")
	var pdHost string
	flag.StringVar(&pdHost, "tikv-pd-host", "http://127.0.0.1:2379", "TiKV PD address (http) for tikv backend")
	flag.StringVar(&pdHost, "u", "http://127.0.0.1:2379", "shorthand for --tikv-pd-host")
	flag.Parse()

	var (
		store   storage.Store
		closeFn func() error
	)

	switch *backend {
	case "tikv":
		tikvStore, err := storage.NewTiKVStore(pdHost)
		if err != nil {
			log.Fatalf("failed to init TiKV store: %v", err)
		}
		store = tikvStore
		closeFn = tikvStore.Close
	case "memory":
		store = storage.NewInMemoryStore()
	default:
		log.Fatalf("unknown backend %q (use tikv|memory)", *backend)
	}

	if closeFn != nil {
		defer func() {
			if err := closeFn(); err != nil {
				log.Printf("backend close error: %v", err)
			}
		}()
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(loggingInterceptor(verbose)))
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

// loggingInterceptor emits request/response bodies for unary RPCs when verbose logging is enabled.
func loggingInterceptor(verbose *bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if verbose != nil && *verbose {
			log.Printf("incoming %s: %+v", info.FullMethod, req)
		}

		resp, err := handler(ctx, req)

		if verbose != nil && *verbose {
			if err != nil {
				log.Printf("reply %s error: %v", info.FullMethod, err)
			} else {
				log.Printf("reply %s: %+v", info.FullMethod, resp)
			}
		}

		return resp, err
	}
}
