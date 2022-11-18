package awskit_test

import (
	"code.olapie.com/errors"
	"context"
	"os"
	"testing"
	"time"

	"code.olapie.com/awskit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func setupS3Bucket(t *testing.T) *awskit.S3Bucket {
	bucket := os.Getenv("S3_TEST_BUCKET")
	require.NotEmpty(t, bucket)
	return awskit.NewS3BucketFromConfig(bucket, loadConfig(t))
}

func TestS3_NotFound(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	exists, err := bucket.Exists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
	_, err = bucket.Get(ctx, id)
	require.EqualError(t, err, errors.NotFound("object %s doesn't exist", id).Error())
	err = bucket.Delete(ctx, id)
	require.NoError(t, err)
	err = bucket.BatchDelete(ctx, []string{id})
	require.NoError(t, err)
}

func TestS3_Put_Empty(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	metadata := map[string]string{"test-key": "test value"}
	err := bucket.Put(ctx, id, nil, metadata)
	require.NoError(t, err)
}

func TestS3_Put_MissingID(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	metadata := map[string]string{"test-key": "test value"}
	err := bucket.Put(ctx, "", nil, metadata)
	require.Error(t, err)
}

func TestS3_Put(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	content := []byte("content" + uuid.NewString())
	metadata := map[string]string{"test-key": "test value"}
	err := bucket.Put(ctx, id, content, metadata)
	require.NoError(t, err)

	readContent, err := bucket.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, content, readContent)

	readMetadata, err := bucket.GetMetadata(ctx, id)
	require.NoError(t, err)
	require.Equal(t, metadata, readMetadata)

	err = bucket.Delete(ctx, id)
	require.NoError(t, err)

	exists, err := bucket.Exists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3_BatchDelete(t *testing.T) {
	r := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var ids []string
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		content := []byte("content" + uuid.NewString())
		metadata := map[string]string{"test-key": "test value"}
		err := r.Put(ctx, id, content, metadata)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	err := r.BatchDelete(ctx, ids)
	require.NoError(t, err)
}
