package awskit

import (
	"bytes"
	"code.olapie.com/conv"
	"context"
	"fmt"
	"net/http"

	"code.olapie.com/ola/httpkit"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	cacheControl = "public, max-age=14400"
)

type S3ACL string

// S3 ACL
const (
	S3Private                S3ACL = "private"
	S3PublicRead             S3ACL = "public-read"
	S3PublicReadWrite        S3ACL = "public-read-write"
	S3AWSExecRead            S3ACL = "aws-exec-read"
	S3AuthenticatedRead      S3ACL = "authenticated-read"
	S3BucketOwnerRead        S3ACL = "bucket-owner-read"
	S3BucketOwnerFullControl S3ACL = "bucket-owner-full-control"
)

type S3 struct {
	bucket       string
	client       *s3.S3
	uploader     *s3manager.Uploader
	downloader   *s3manager.Downloader
	ACL          *string
	CacheControl string
}

func NewS3(bucket string, ses *session.Session) *S3 {
	return &S3{
		bucket:       bucket,
		client:       s3.New(ses),
		uploader:     s3manager.NewUploader(ses),
		downloader:   s3manager.NewDownloader(ses),
		ACL:          aws.String(string(S3Private)),
		CacheControl: cacheControl,
	}
}

func (s *S3) Upload(ctx context.Context, id string, content []byte, metadata map[string]string) (location string, err error) {
	input := &s3manager.UploadInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(id),
		Body:         bytes.NewBuffer(content),
		ACL:          s.ACL,
		CacheControl: aws.String(s.CacheControl),
		ContentType:  aws.String(httpkit.DetectMIMEType(content)),
	}

	input.Metadata = make(map[string]*string, len(metadata))
	for k, v := range metadata {
		input.Metadata[k] = aws.String(v)
	}

	res, err := s.uploader.UploadWithContext(ctx, input)
	if err != nil {
		return "", fmt.Errorf("cannot upload %s: %w", id, err)
	}
	return res.Location, nil
}

func (s *S3) Download(ctx context.Context, id string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(id),
	}
	w := aws.NewWriteAtBuffer(make([]byte, 0, 50*1024))
	_, err := s.downloader.DownloadWithContext(ctx, w, input)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (s *S3) Exists(ctx context.Context, id string) (bool, error) {
	_, err := s.getHeadObject(ctx, id)
	if err == nil {
		return true, nil
	}

	if failure, ok := err.(awserr.RequestFailure); ok {
		if failure.StatusCode() == http.StatusNotFound {
			return false, nil
		}
	}
	return false, err
}

func (s *S3) Delete(ctx context.Context, id string) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(id),
	})

	if err != nil {
		return fmt.Errorf("cannot delete object: %w", err)
	}

	err = s.client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(id),
	})

	if err != nil {
		return fmt.Errorf("object still exists: %w", err)
	}

	return nil
}

func (s *S3) BatchDelete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &s3.Delete{
			Objects: conv.MustSlice(ids, func(id string) *s3.ObjectIdentifier {
				return &s3.ObjectIdentifier{
					Key: aws.String(id),
				}
			}),
		},
	}

	output, err := s.client.DeleteObjectsWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("cannot delete objects: %w", err)
	}

	if len(output.Deleted) == 0 {
		return nil
	}

	if len(output.Deleted) != len(ids) {
		idSet := conv.MustSliceToSet[string, string](ids, nil)
		for _, del := range output.Deleted {
			delete(idSet, *del.Key)
		}
		if len(idSet) != 0 {
			return fmt.Errorf("some ids cannot be deleted: %v", conv.GetMapKeys(idSet))
		}
	}

	err = s.client.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    output.Deleted[0].Key,
	})

	if err != nil {
		return fmt.Errorf("wait until object not exists: %w", err)
	}
	return nil
}

func (s *S3) GetMetadata(ctx context.Context, id string) (map[string]string, error) {
	head, err := s.getHeadObject(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("cannot get head object: %w", err)
	}

	metadata := map[string]string{}
	for k, v := range head.Metadata {
		if v != nil {
			metadata[k] = *v
		}
	}
	return metadata, nil
}

func (s *S3) getHeadObject(ctx context.Context, id string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(id),
	}
	return s.client.HeadObjectWithContext(ctx, input)
}
