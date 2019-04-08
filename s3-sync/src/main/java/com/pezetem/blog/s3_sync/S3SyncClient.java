package com.pezetem.blog.s3_sync;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Event;

public class S3SyncClient implements RequestHandler<com.amazonaws.services.lambda.runtime.events.S3Event, String> {

    @Override
    public String handleRequest(com.amazonaws.services.lambda.runtime.events.S3Event notification, Context context) {
        String dstBucket = System.getenv("DESTINATION_BUCKET");

        AmazonS3 amazonS3 = AmazonS3Client.builder().build();

        notification.getRecords()
                .forEach(event -> syncObject(dstBucket, amazonS3, event, "s3:" + event.getEventName()));

        return "Finished processing";
    }

    private void syncObject(String dstBucket, AmazonS3 amazonS3, S3EventNotification.S3EventNotificationRecord event, String s3Event) {
        if (s3CreatedObjectEvent(s3Event)) {
            String srcObject = event.getS3().getObject().getKey();
            String srcBucket = event.getS3().getBucket().getName();

            amazonS3.copyObject(srcBucket, srcObject, dstBucket, srcObject);
            return;
        }
        if (s3DeletedObjectEvent(s3Event)) {
            amazonS3.deleteObject(dstBucket, event.getS3().getObject().getKey());
            return;
        }

        throw new IllegalStateException("Unsupported s3 event");
    }

    private boolean s3CreatedObjectEvent(String s3Event) {
        return S3Event.ObjectCreated.toString().equals(s3Event) ||
                S3Event.ObjectCreatedByCompleteMultipartUpload.toString().equals(s3Event) ||
                S3Event.ObjectCreatedByCopy.toString().equals(s3Event) ||
                S3Event.ObjectCreatedByPost.toString().equals(s3Event) ||
                S3Event.ObjectCreatedByPut.toString().equals(s3Event);
    }

    private boolean s3DeletedObjectEvent(String s3Event) {
        return S3Event.ObjectRemoved.toString().equals(s3Event) ||
                S3Event.ObjectRemovedDelete.toString().equals(s3Event) ||
                S3Event.ObjectRemovedDeleteMarkerCreated.toString().equals(s3Event);
    }
}
