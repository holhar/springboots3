package de.holhar.aws.springboots3;

import java.net.URL;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3Object {

  String name;
  String key;
  URL url;

  @Builder.Default
  boolean isPublic = false;
}
