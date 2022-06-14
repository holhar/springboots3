package de.holhar.aws.springboots3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.waiters.WaiterParameters;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class S3Repository {

  private final AmazonS3Client s3Client;

  public S3Repository(AmazonS3Client s3Client) {
    this.s3Client = s3Client;
  }

  public void createBucket(String bucket) {
    s3Client.createBucket(bucket);
    log.info("Sent request to create bucket '{}'", bucket);

    s3Client.waiters().bucketExists().run(new WaiterParameters<>(new HeadBucketRequest(bucket)));
    log.info("Bucket '{}' is ready", bucket);
  }

  public void deleteBucket(String bucket) {
    s3Client.deleteBucket(bucket);
    log.info("Sent request to delete bucket '{}'", bucket);

    s3Client.waiters().bucketNotExists().run(new WaiterParameters<>(new HeadBucketRequest(bucket)));
    log.info("Bucket '{}' successfully deleted", bucket);
  }

  public List<S3Object> getS3Objects(String bucket) {
    List<S3Object> result = s3Client.listObjectsV2(bucket).getObjectSummaries().stream()
        .map(S3ObjectSummary::getKey)
        .map(key -> mapS3ToObject(bucket, key))
        .collect(Collectors.toList());
    log.info("Found '{}' objects in the bucket '{}'", result.size(), bucket);
    return result;
  }

  public S3Object safe(String bucket, String key, String name, InputStream payload) {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.addUserMetadata("name", name);
    s3Client.putObject(bucket, key, payload, metadata);
    log.info("Sent putObject request for object with key '{}'", key);
    return S3Object.builder()
        .name(name)
        .key(key)
        .url(s3Client.getUrl(bucket, key))
        .build();
  }

  public void makePublic(String bucket, String key) {
    s3Client.setObjectAcl(bucket, key, CannedAccessControlList.PublicRead);
    log.info("Sent request to make object in bucket '{}' with key '{}' public", bucket, key);
  }

  public void makePrivate(String bucket, String key) {
    s3Client.setObjectAcl(bucket, key, CannedAccessControlList.BucketOwnerFullControl);
    log.info("Sent request to make object in bucket '{}' with key '{}' private", bucket, key);
  }

  private S3Object mapS3ToObject(String bucket, String key) {
    return S3Object.builder()
        .name(s3Client.getObjectMetadata(bucket, key).getUserMetaDataOf("name"))
        .key(key)
        .url(s3Client.getUrl(bucket, key))
        .isPublic(
            s3Client.getObjectAcl(bucket, key).getGrantsAsList().stream()
                .anyMatch(grant -> grant.equals(S3Repository.publicObjectReadGrant())))
        .build();
  }

  private static Grant publicObjectReadGrant() {
    return new Grant(
        GroupGrantee.parseGroupGrantee(GroupGrantee.AllUsers.getIdentifier()), Permission.Read);
  }
}
