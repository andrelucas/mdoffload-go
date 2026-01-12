package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	mdoffloadv1 "github.com/andrelucas/mdoffload-go/bits.linode.com/LinodeApi/obj-endpoint/gen/proto/mdoffload/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8004", "mdoffload server address (host:port)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-RPC timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		printMainUsage()
		os.Exit(1)
	}

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := mdoffloadv1.NewMDOffloadServiceClient(conn)

	var runErr error
	switch cmd {
	case "get-bucket":
		runErr = runGetBucket(client, *timeout, args)
	case "set-bucket":
		runErr = runSetBucket(client, *timeout, args)
	case "purge-bucket":
		runErr = runPurgeBucket(client, *timeout, args)
	case "get-object":
		runErr = runGetObject(client, *timeout, args)
	case "set-object":
		runErr = runSetObject(client, *timeout, args)
	case "purge-object":
		runErr = runPurgeObject(client, *timeout, args)
	default:
		printMainUsage()
		os.Exit(1)
	}

	if runErr != nil {
		log.Fatalf("%s failed: %v", cmd, runErr)
	}
}

func printMainUsage() {
	fmt.Fprintf(os.Stderr, "Usage: mdoffload-client [--addr host:port] [--timeout 5s] <command> [args]\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  get-bucket     Fetch bucket attributes\n")
	fmt.Fprintf(os.Stderr, "  set-bucket     Add/remove bucket attributes\n")
	fmt.Fprintf(os.Stderr, "  purge-bucket   Remove all bucket attributes\n")
	fmt.Fprintf(os.Stderr, "  get-object     Fetch object attributes\n")
	fmt.Fprintf(os.Stderr, "  set-object     Add/remove object attributes\n")
	fmt.Fprintf(os.Stderr, "  purge-object   Remove all object attributes\n")
}

func runGetBucket(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("get-bucket", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := client.GetBucketAttributes(ctx, &mdoffloadv1.GetBucketAttributesRequest{
		BucketId:   *bucketID,
		BucketName: *bucketName,
		UserId:     *userID,
	})
	if err != nil {
		return err
	}

	if len(resp.GetAttributes()) == 0 {
		fmt.Println("(empty)")
		return nil
	}

	for k, v := range resp.GetAttributes() {
		fmt.Printf("%s=%s\n", k, string(v))
	}
	return nil
}

func runSetBucket(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("set-bucket", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	replace := fs.Bool("replace", false, "replace existing attributes instead of merging")
	var adds stringSlice
	var deletes stringSlice
	fs.Var(&adds, "add", "attribute to add as key=value (repeatable)")
	fs.Var(&deletes, "delete", "attribute key to delete (repeatable)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	attrs, err := parseAttributes(adds)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = client.SetBucketAttributes(ctx, &mdoffloadv1.SetBucketAttributesRequest{
		BucketId:                  *bucketID,
		BucketName:                *bucketName,
		UserId:                    *userID,
		AttributesToAdd:           attrs,
		AttributesToDelete:        deletes,
		ReplaceExistingAttributes: *replace,
	})
	return err
}

func runPurgeBucket(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("purge-bucket", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := client.PurgeBucketAttributes(ctx, &mdoffloadv1.PurgeBucketAttributesRequest{
		BucketId:   *bucketID,
		BucketName: *bucketName,
		UserId:     *userID,
	})
	return err
}

func runGetObject(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("get-object", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	objectKey := fs.String("object-key", "", "object key")
	fs.StringVar(objectKey, "o", "", "object key (shorthand)")
	instanceID := fs.String("instance-id", "", "object instance ID (optional)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := client.GetObjectAttributes(ctx, &mdoffloadv1.GetObjectAttributesRequest{
		BucketId:         *bucketID,
		BucketName:       *bucketName,
		UserId:           *userID,
		ObjectKey:        *objectKey,
		ObjectInstanceId: *instanceID,
	})
	if err != nil {
		return err
	}

	if len(resp.GetAttributes()) == 0 {
		fmt.Println("(empty)")
		return nil
	}

	for k, v := range resp.GetAttributes() {
		fmt.Printf("%s=%s\n", k, string(v))
	}
	return nil
}

func runSetObject(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("set-object", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	objectKey := fs.String("object-key", "", "object key")
	fs.StringVar(objectKey, "o", "", "object key (shorthand)")
	instanceID := fs.String("instance-id", "", "object instance ID (optional)")
	newInstance := fs.Bool("new-instance", false, "treat as a new object instance (clears existing attributes)")
	var adds stringSlice
	var deletes stringSlice
	fs.Var(&adds, "add", "attribute to add as key=value (repeatable)")
	fs.Var(&deletes, "delete", "attribute key to delete (repeatable)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	attrs, err := parseAttributes(adds)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = client.SetObjectAttributes(ctx, &mdoffloadv1.SetObjectAttributesRequest{
		BucketId:           *bucketID,
		BucketName:         *bucketName,
		UserId:             *userID,
		ObjectKey:          *objectKey,
		ObjectInstanceId:   *instanceID,
		AttributesToAdd:    attrs,
		AttributesToDelete: deletes,
		NewObjectInstance:  *newInstance,
	})
	return err
}

func runPurgeObject(client mdoffloadv1.MDOffloadServiceClient, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("purge-object", flag.ContinueOnError)
	bucketID := fs.String("bucket-id", "", "bucket ID")
	bucketName := fs.String("bucket-name", "", "bucket name")
	fs.StringVar(bucketID, "i", "", "bucket ID (shorthand)")
	fs.StringVar(bucketName, "b", "", "bucket name (shorthand)")
	userID := fs.String("user-id", "", "user ID (optional scope for bucket name)")
	objectKey := fs.String("object-key", "", "object key")
	fs.StringVar(objectKey, "o", "", "object key (shorthand)")
	instanceID := fs.String("instance-id", "", "object instance ID (optional)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := client.PurgeObjectAttributes(ctx, &mdoffloadv1.PurgeObjectAttributesRequest{
		BucketId:         *bucketID,
		BucketName:       *bucketName,
		UserId:           *userID,
		ObjectKey:        *objectKey,
		ObjectInstanceId: *instanceID,
	})
	return err
}

func parseAttributes(values []string) (map[string][]byte, error) {
	attrs := map[string][]byte{}
	for _, raw := range values {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 {
			return nil, errors.New("attributes must be key=value")
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			return nil, errors.New("attribute key is required")
		}
		attrs[key] = []byte(parts[1])
	}
	return attrs, nil
}
