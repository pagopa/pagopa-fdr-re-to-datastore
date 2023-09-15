package it.gov.pagopa.fdrretodatastore;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReEvent {
    private String uniqueId;
    private String serviceIdentifier;
    private String created;
    private String sessionId;
    private String eventType;
    private String fdr;
    private String pspId;
    private String organizationId;
    private String fdrAction;
    private String httpType;
    private String httpMethod;
    private String httpUrl;
    private String payload;
    private BlobHttpBody blobBodyRef;
    private Map<String, List<String>> header;
    private boolean fdrPhysicalDelete;
    private String fdrStatus;
    private Long revision;

}
