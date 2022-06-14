package de.holhar.aws.springboots3;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@Slf4j
@RequestMapping("/api/v1")
public class S3RestAdapter {

  private final S3Repository s3Repository;

  public S3RestAdapter(S3Repository s3Repository) {
    this.s3Repository = s3Repository;
  }

  @PostMapping("/{bucketName}")
  public ResponseEntity<Void> createBucket(@PathVariable String bucketName) {
    s3Repository.createBucket(bucketName);
    return ResponseEntity.ok().build();
  }

  @PostMapping("/bucket/{bucketName}/object")
  public ResponseEntity<S3Object> uploadFile(
      @PathVariable String bucketName,
      @RequestParam("file") MultipartFile file,
      @RequestParam(required = false, name = "fileName") String fileName) throws IOException {
    var key = fileName != null ? fileName : file.getOriginalFilename();
    S3Object s3Object = s3Repository.safe(bucketName, key, key, file.getInputStream());
    return ResponseEntity.ok(s3Object);
  }

  @PostMapping("/{bucketName}/object/{key}")
  public ResponseEntity<Void> publishFile(@PathVariable String bucketName, @PathVariable String key) {
    s3Repository.makePublic(bucketName, key);
    return ResponseEntity.ok().build();
  }
}
