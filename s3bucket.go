package awskit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"code.olapie.com/sugar/v2/maps"
	"code.olapie.com/sugar/v2/rt"
	"code.olapie.com/sugar/v2/slices"
	"code.olapie.com/sugar/v2/xerror"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssigner "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	cacheControl = "public, max-age=14400"
)

var s3ErrorNotFound = &types.NotFound{}
var _ error = s3ErrorNotFound

type S3Bucket struct {
	bucket             string
	client             *s3.Client
	presignClient      *s3.PresignClient
	objExistsWaiter    *s3.ObjectExistsWaiter
	objNotExistsWaiter *s3.ObjectNotExistsWaiter

	ACL          types.ObjectCannedACL
	CacheControl string
}

func NewS3Bucket(bucket string, c *s3.Client) *S3Bucket {
	s := &S3Bucket{
		bucket:        bucket,
		client:        c,
		presignClient: s3.NewPresignClient(c),

		ACL:          types.ObjectCannedACLPrivate,
		CacheControl: cacheControl,
	}
	s.objExistsWaiter = s3.NewObjectExistsWaiter(s.client)
	s.objNotExistsWaiter = s3.NewObjectNotExistsWaiter(s.client)
	return s
}

func NewS3BucketFromConfig(bucket string, cfg aws.Config, options ...func(*s3.Options)) *S3Bucket {
	return NewS3Bucket(bucket, s3.NewFromConfig(cfg, options...))
}

func (s *S3Bucket) Put(ctx context.Context, key string, content []byte, metadata map[string]string, optFns ...func(input *s3.PutObjectInput)) (string, error) {
	input := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		Body:         bytes.NewBuffer(content),
		ACL:          s.ACL,
		CacheControl: aws.String(s.CacheControl),
		ContentType:  aws.String(http.DetectContentType(content)),
		Metadata:     metadata,
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	return rt.Dereference(output.ETag), nil
}

func (s *S3Bucket) Get(ctx context.Context, key string, optFns ...func(input *s3.GetObjectInput)) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	for _, fn := range optFns {
		fn(input)
	}

	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, xerror.NotFound("object %s doesn't exist", key)
		}
		return nil, fmt.Errorf("s3.GetObject: %w", err)
	}

	content, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("io.ReadAll: %w", err)
	}
	output.Body.Close()

	return content, nil
}

func (s *S3Bucket) CreateMultipartUpload(ctx context.Context, key string, optFns ...func(*s3.CreateMultipartUploadInput)) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ACL:          s.ACL,
		CacheControl: aws.String(s.CacheControl),
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", err
	}
	return *output.UploadId, nil
}

func (s *S3Bucket) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []types.CompletedPart, optFns ...func(*s3.CompleteMultipartUploadInput)) (string, error) {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return "", err
	}
	return rt.Dereference(output.ETag), nil
}

func (s *S3Bucket) AbortMultipartUpload(ctx context.Context, key, uploadID string, optFns ...func(*s3.AbortMultipartUploadInput)) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}
	for _, fn := range optFns {
		fn(input)
	}
	_, err := s.client.AbortMultipartUpload(ctx, input)
	return err
}

func (s *S3Bucket) ListMultipartUploads(ctx context.Context, optFns ...func(*s3.ListMultipartUploadsInput)) ([]types.MultipartUpload, error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucket),
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.ListMultipartUploads(ctx, input)
	if err != nil {
		return nil, err
	}
	return output.Uploads, nil
}

// ListParts only works if upload is not completed or aborted
func (s *S3Bucket) ListParts(ctx context.Context, key, uploadID string, optFns ...func(input *s3.ListPartsInput)) ([]types.Part, error) {
	input := &s3.ListPartsInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.ListParts(ctx, input)
	if err != nil {
		return nil, err
	}
	return output.Parts, nil
}

func (s *S3Bucket) PreSignUploadPart(ctx context.Context, key, uploadID string, part int, ttl time.Duration, optFns ...func(*s3.UploadPartInput)) (*awssigner.PresignedHTTPRequest, error) {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(key),
		PartNumber: int32(part),
		UploadId:   aws.String(uploadID),
	}
	for _, fn := range optFns {
		fn(input)
	}
	return s.presignClient.PresignUploadPart(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
}

func (s *S3Bucket) PreSignGet(ctx context.Context, key string, ttl time.Duration, optFns ...func(*s3.GetObjectInput)) (*awssigner.PresignedHTTPRequest, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	for _, fn := range optFns {
		fn(input)
	}
	return s.presignClient.PresignGetObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
}

func (s *S3Bucket) PreSignPut(ctx context.Context, key string, ttl time.Duration, optFns ...func(*s3.PutObjectInput)) (*awssigner.PresignedHTTPRequest, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	for _, fn := range optFns {
		fn(input)
	}
	return s.presignClient.PresignPutObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
}

func (s *S3Bucket) Delete(ctx context.Context, key string, optFns ...func(*s3.DeleteObjectInput)) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	_, err := s.client.DeleteObject(ctx, input)

	for _, fn := range optFns {
		fn(input)
	}

	if err != nil {
		return fmt.Errorf("s3.DeleteObject: %w", err)
	}

	err = s.objNotExistsWaiter.Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, time.Second*5)
	if err != nil {
		return fmt.Errorf("s3.ObjectNotExistsWaiter.Wait: %w", err)
	}
	return nil
}

func (s *S3Bucket) BatchDelete(ctx context.Context, ids []string, optFns ...func(*s3.DeleteObjectsInput)) error {
	if len(ids) == 0 {
		return nil
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: slices.MustTransform(ids, func(key string) types.ObjectIdentifier {
				return types.ObjectIdentifier{
					Key: aws.String(key),
				}
			}),
		},
	}

	for _, fn := range optFns {
		fn(input)
	}

	output, err := s.client.DeleteObjects(ctx, input)
	if err != nil {
		return fmt.Errorf("s3.DeleteObjects: %w", err)
	}

	if len(output.Deleted) == 0 {
		return nil
	}

	if len(output.Deleted) != len(ids) {
		idSet := slices.ToSet(ids)
		for _, del := range output.Deleted {
			delete(idSet, *del.Key)
		}
		if len(idSet) != 0 {
			return fmt.Errorf("some ids cannot be deleted: %v", maps.GetKeys(idSet))
		}
	}

	err = s.objNotExistsWaiter.Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ids[0]),
	}, time.Second*5)
	if err != nil {
		return fmt.Errorf("s3.ObjectNotExistsWaiter.Wait: %w", err)
	}
	return nil
}

func (s *S3Bucket) GetHeadObject(ctx context.Context, key string, optFns ...func(*s3.HeadObjectInput)) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	for _, fn := range optFns {
		fn(input)
	}
	output, err := s.client.HeadObject(ctx, input)
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			if apiError.ErrorCode() == s3ErrorNotFound.ErrorCode() {
				return nil, ErrKeyNotFound
			}
		}
		return nil, err
	}
	return output, nil
}
