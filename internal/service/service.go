package service

import (
	"context"
	"errors"

	mdoffloadv1 "github.com/andrelucas/mdoffload-go/bits.linode.com/LinodeApi/obj-endpoint/gen/proto/mdoffload/v1"
	"github.com/andrelucas/mdoffload-go/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MDOffloadServer implements the MDOffloadService RPCs against an attribute store.
type MDOffloadServer struct {
	mdoffloadv1.UnimplementedMDOffloadServiceServer
	store storage.Store
}

// NewMDOffloadServer returns a new MDOffloadService server backed by the provided Store.
func NewMDOffloadServer(store storage.Store) *MDOffloadServer {
	return &MDOffloadServer{store: store}
}

func (s *MDOffloadServer) GetBucketAttributes(ctx context.Context, req *mdoffloadv1.GetBucketAttributesRequest) (*mdoffloadv1.GetBucketAttributesResponse, error) {
	ref := bucketRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName())
	attrs, err := s.store.GetBucketAttributes(ctx, ref)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.GetBucketAttributesResponse{Attributes: attrs}, nil
}

func (s *MDOffloadServer) SetBucketAttributes(ctx context.Context, req *mdoffloadv1.SetBucketAttributesRequest) (*mdoffloadv1.SetBucketAttributesResponse, error) {
	ref := bucketRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName())
	err := s.store.SetBucketAttributes(ctx, ref, req.GetAttributesToAdd(), req.GetAttributesToDelete(), req.GetReplaceExistingAttributes())
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.SetBucketAttributesResponse{}, nil
}

func (s *MDOffloadServer) PurgeBucketAttributes(ctx context.Context, req *mdoffloadv1.PurgeBucketAttributesRequest) (*mdoffloadv1.PurgeBucketAttributesResponse, error) {
	ref := bucketRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName())
	if err := s.store.PurgeBucketAttributes(ctx, ref); err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.PurgeBucketAttributesResponse{}, nil
}

func (s *MDOffloadServer) GetObjectAttributes(ctx context.Context, req *mdoffloadv1.GetObjectAttributesRequest) (*mdoffloadv1.GetObjectAttributesResponse, error) {
	ref := objectRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName(), req.GetObjectKey(), req.GetObjectInstanceId())
	attrs, err := s.store.GetObjectAttributes(ctx, ref)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.GetObjectAttributesResponse{Attributes: attrs}, nil
}

func (s *MDOffloadServer) SetObjectAttributes(ctx context.Context, req *mdoffloadv1.SetObjectAttributesRequest) (*mdoffloadv1.SetObjectAttributesResponse, error) {
	ref := objectRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName(), req.GetObjectKey(), req.GetObjectInstanceId())
	err := s.store.SetObjectAttributes(ctx, ref, req.GetAttributesToAdd(), req.GetAttributesToDelete(), req.GetNewObjectInstance())
	if err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.SetObjectAttributesResponse{}, nil
}

func (s *MDOffloadServer) PurgeObjectAttributes(ctx context.Context, req *mdoffloadv1.PurgeObjectAttributesRequest) (*mdoffloadv1.PurgeObjectAttributesResponse, error) {
	ref := objectRefFromRequest(req.GetUserId(), req.GetBucketId(), req.GetBucketName(), req.GetObjectKey(), req.GetObjectInstanceId())
	if err := s.store.PurgeObjectAttributes(ctx, ref); err != nil {
		return nil, mapStorageError(err)
	}
	return &mdoffloadv1.PurgeObjectAttributesResponse{}, nil
}

func bucketRefFromRequest(userID, bucketID, bucketName string) storage.BucketRef {
	return storage.BucketRef{UserID: userID, BucketID: bucketID, BucketName: bucketName}
}

func objectRefFromRequest(userID, bucketID, bucketName, objectKey, instanceID string) storage.ObjectRef {
	return storage.ObjectRef{
		Bucket:           storage.BucketRef{UserID: userID, BucketID: bucketID, BucketName: bucketName},
		ObjectKey:        objectKey,
		ObjectInstanceID: instanceID,
	}
}

func mapStorageError(err error) error {
	switch {
	case errors.Is(err, storage.ErrBucketReferenceMissing), errors.Is(err, storage.ErrBucketIDMissing), errors.Is(err, storage.ErrObjectKeyMissing):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "storage error: %v", err)
	}
}
