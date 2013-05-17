#!/bin/sh

VERSION=`cat VERSION`
TARGET="squall_worker-$VERSION"
LATEST_NAME="squall_worker-latest"

echo "Building version $VERSION"
go clean
GOOS=linux GOARCH=386 CGO_ENABLED=0  go build -o $TARGET

# Put a versioned copy...
s3cmd --acl-public put $TARGET s3://bradhe-share/$TARGET
s3cmd --acl-public put $TARGET s3://bradhe-share/$LATEST_NAME
