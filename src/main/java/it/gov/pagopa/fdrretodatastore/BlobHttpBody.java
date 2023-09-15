package it.gov.pagopa.fdrretodatastore;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BlobHttpBody {

  private String storageAccount;
  private String containerName;
  private String fileName;

  private long fileLength;
}
