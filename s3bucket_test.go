package awskit_test

import (
	"context"
	"os"
	"testing"
	"time"

	"code.olapie.com/awskit"
	"code.olapie.com/sugar/v2/xerror"
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
	_, err := bucket.GetHeadObject(ctx, id)
	require.Error(t, err)
	require.True(t, xerror.IsNotExist(err))
	_, err = bucket.Get(ctx, id)
	require.EqualError(t, err, xerror.NotFound("object %s doesn't exist", id).Error())
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
	_, err := bucket.Put(ctx, id, nil, metadata)
	require.NoError(t, err)
}

func TestS3_Put_MissingID(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	metadata := map[string]string{"test-key": "test value"}
	_, err := bucket.Put(ctx, "", nil, metadata)
	require.Error(t, err)
}

func TestS3_Put(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	content := []byte("content" + uuid.NewString())
	metadata := map[string]string{"test-key": "test value"}
	_, err := bucket.Put(ctx, id, content, metadata)
	require.NoError(t, err)

	readContent, err := bucket.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, content, readContent)

	head, err := bucket.GetHeadObject(ctx, id)
	require.NoError(t, err)
	require.Equal(t, metadata, head.Metadata)

	err = bucket.Delete(ctx, id)
	require.NoError(t, err)

	_, err = bucket.GetHeadObject(ctx, id)
	require.True(t, xerror.IsNotExist(err))
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
		_, err := r.Put(ctx, id, content, metadata)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	err := r.BatchDelete(ctx, ids)
	require.NoError(t, err)
}
