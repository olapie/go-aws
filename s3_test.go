package awskit_test

import (
	"context"
	"os"
	"testing"
	"time"

	"code.olapie.com/awskit"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func setupS3(t *testing.T) *awskit.S3 {
	bucket := os.Getenv("S3_TEST_BUCKET")
	require.NotEmpty(t, bucket)
	return awskit.NewS3(bucket, session.Must(session.NewSession()))
}

func TestS3_Upload(t *testing.T) {
	r := setupS3(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	content := []byte("content" + uuid.NewString())
	metadata := map[string]string{"Testkey": "test value"}
	location, err := r.Upload(ctx, id, content, metadata)
	require.NoError(t, err)
	require.NotEmpty(t, location)
	t.Log(location)

	readContent, err := r.Download(ctx, id)
	require.NoError(t, err)
	require.Equal(t, content, readContent)

	readMetadata, err := r.GetMetadata(ctx, id)
	require.NoError(t, err)
	require.Equal(t, metadata, readMetadata)

	err = r.Delete(ctx, id)
	require.NoError(t, err)

	exists, err := r.Exists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3_BatchDelete(t *testing.T) {
	r := setupS3(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var ids []string
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		content := []byte("content" + uuid.NewString())
		metadata := map[string]string{"Testkey": "test value"}
		_, err := r.Upload(ctx, id, content, metadata)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	err := r.BatchDelete(ctx, ids)
	require.NoError(t, err)
}
