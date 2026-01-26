package service

import (
	"context"
	"reflect"
	"testing"

	mdoffloadv1 "github.com/andrelucas/mdoffload-go/bits.linode.com/LinodeApi/obj-endpoint/gen/proto/mdoffload/v1"
	"github.com/andrelucas/mdoffload-go/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestServer() *MDOffloadServer {
    return NewMDOffloadServer(storage.NewInMemoryStore())
}

func TestBucketIDRequired(t *testing.T) {
    srv := newTestServer()
    _, err := srv.GetBucketAttributes(context.Background(), &mdoffloadv1.GetBucketAttributesRequest{BucketName: "shared"})
    if err == nil {
        t.Fatalf("expected error for missing bucket_id")
    }
    if status.Code(err) != codes.InvalidArgument {
        t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
    }
}

func TestBucketIDUniqueness(t *testing.T) {
    ctx := context.Background()
    srv := newTestServer()

    put := func(bucketID, val string) {
        req := &mdoffloadv1.SetBucketAttributesRequest{
            BucketId:                 bucketID,
            BucketName:               "shared-name",
            AttributesToAdd:          map[string][]byte{"color": []byte(val)},
            ReplaceExistingAttributes: false,
        }
        if _, err := srv.SetBucketAttributes(ctx, req); err != nil {
            t.Fatalf("set attrs for bucket %s: %v", bucketID, err)
        }
    }

    get := func(bucketID string) map[string][]byte {
        resp, err := srv.GetBucketAttributes(ctx, &mdoffloadv1.GetBucketAttributesRequest{
            BucketId:   bucketID,
            BucketName: "shared-name",
        })
        if err != nil {
            t.Fatalf("get attrs for bucket %s: %v", bucketID, err)
        }
        return resp.GetAttributes()
    }

    put("bucket-1", "blue")
    put("bucket-2", "red")

    attrs1 := get("bucket-1")
    attrs2 := get("bucket-2")

    assertEqualAttrs(t, map[string][]byte{"color": []byte("blue")}, attrs1)
    assertEqualAttrs(t, map[string][]byte{"color": []byte("red")}, attrs2)
}

func TestObjectInstanceIsolation(t *testing.T) {
    ctx := context.Background()
    srv := newTestServer()

    set := func(instanceID, key, val string) {
        _, err := srv.SetObjectAttributes(ctx, &mdoffloadv1.SetObjectAttributesRequest{
            BucketId:          "bucket-1",
            ObjectKey:         "object-key",
            ObjectInstanceId:  instanceID,
            AttributesToAdd:   map[string][]byte{key: []byte(val)},
            NewObjectInstance: true,
        })
        if err != nil {
            t.Fatalf("set attrs for instance %s: %v", instanceID, err)
        }
    }

    get := func(instanceID string) map[string][]byte {
        resp, err := srv.GetObjectAttributes(ctx, &mdoffloadv1.GetObjectAttributesRequest{
            BucketId:         "bucket-1",
            ObjectKey:        "object-key",
            ObjectInstanceId: instanceID,
        })
        if err != nil {
            t.Fatalf("get attrs for instance %s: %v", instanceID, err)
        }
        return resp.GetAttributes()
    }

    set("inst-1", "size", "small")
    set("inst-2", "size", "large")

    attrs1 := get("inst-1")
    attrs2 := get("inst-2")

    assertEqualAttrs(t, map[string][]byte{"size": []byte("small")}, attrs1)
    assertEqualAttrs(t, map[string][]byte{"size": []byte("large")}, attrs2)
}

func assertEqualAttrs(t *testing.T, want, got map[string][]byte) {
    t.Helper()
    if !reflect.DeepEqual(want, got) {
        t.Fatalf("attributes mismatch\nwant: %#v\n got: %#v", want, got)
    }
}
