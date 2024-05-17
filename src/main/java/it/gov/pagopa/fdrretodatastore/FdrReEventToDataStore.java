package it.gov.pagopa.fdrretodatastore;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import it.gov.pagopa.fdrretodatastore.exception.AppException;
import it.gov.pagopa.fdrretodatastore.util.ObjectMapperUtils;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Azure Functions with Azure Queue trigger.
 */
public class FdrReEventToDataStore {
    /**
     * This function will be invoked when an Event Hub trigger occurs
     */

	private static final Integer MAX_RETRY_COUNT = 5;

	private Pattern replaceDashPattern = Pattern.compile("-([a-zA-Z])");
	private static String idField = "uniqueId";
	private static String tableName = System.getenv("TABLE_STORAGE_TABLE_NAME");
	private static String columnCreated = "created";
	private static String partitionKeyColumnCreated = "PartitionKey";
	private static String serviceIdentifier = "serviceIdentifier";
	private static String serviceIDFdr001 = "FDR001";

	private static MongoClient mongoClient = null;

	private static TableServiceClient tableServiceClient = null;

	private static MongoClient getMongoClient(){
		if(mongoClient==null){
			mongoClient = new MongoClient(new MongoClientURI(System.getenv("COSMOS_CONN_STRING")));
		}
		return mongoClient;
	}

	private static TableServiceClient getTableServiceClient(){
		if(tableServiceClient==null){
			tableServiceClient = new TableServiceClientBuilder().connectionString(System.getenv("TABLE_STORAGE_CONN_STRING"))
					.buildClient();
			tableServiceClient.createTableIfNotExists(tableName);
		}
		return tableServiceClient;
	}


	private void toTableStorage(TableClient tableClient,Map<String,Object> reEvent) throws JsonProcessingException {
		for(Map.Entry<String, Object> entry: reEvent.entrySet()){
			if(entry.getValue() instanceof Map){
				reEvent.put(entry.getKey(),ObjectMapperUtils.writeValueAsString(entry.getValue()));
			}
		}
		TableEntity entity = new TableEntity((String)reEvent.get(partitionKeyColumnCreated), (String)reEvent.get(idField));
		entity.setProperties(reEvent);
		tableClient.createEntity(entity);
	}

	private String replaceDashWithUppercase(String input) {
		if(!input.contains("-")){
			return input;
		}
		Matcher matcher = replaceDashPattern.matcher(input);
		StringBuffer sb = new StringBuffer();

		while (matcher.find()) {
			matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
		}
		matcher.appendTail(sb);

		return sb.toString();
	}

    @FunctionName("EventHubFdrReEventProcessor")
	@ExponentialBackoffRetry(maxRetryCount = 5, maximumInterval = "00:15:00", minimumInterval = "00:00:10")
	public void processNodoReEvent (
            @EventHubTrigger(
                    name = "FdrReEvent",
                    eventHubName = "", // blank because the value is included in the connection string
                    connection = "EVENTHUB_CONN_STRING",
                    cardinality = Cardinality.MANY)
    		List<String> reEvents,
    		@BindingName(value = "PropertiesArray") Map<String, Object>[] properties,
            final ExecutionContext context) {

		String errorCause = null;
		boolean isPersistenceOk = true;
		int retryIndex = context.getRetryContext() == null ? -1 : context.getRetryContext().getRetrycount();

		Logger logger = context.getLogger();
		logger.log(Level.FINE, () -> String.format("Persisting [%d] events...", reEvents.size()));
		if (retryIndex == MAX_RETRY_COUNT) {
			logger.log(Level.WARNING, () -> String.format("[ALERT][LAST RETRY][FdrREToDS] Performing last retry for event ingestion: InvocationId [%s], Events: %s", context.getInvocationId(), reEvents));
		}

		MongoDatabase database = getMongoClient().getDatabase(System.getenv("COSMOS_DB_NAME"));
		MongoCollection<Document> collection = database.getCollection(System.getenv("COSMOS_DB_COLLECTION_NAME"));
		TableClient tableClient = getTableServiceClient().getTableClient(tableName);

        try {
        	if (reEvents.size() == properties.length) {
				for (int index=0; index< properties.length; index++){
					final Map<String,Object> reEvent = ObjectMapperUtils.readValue(reEvents.get(index), Map.class);
					Object servId = reEvent.get(serviceIdentifier);
					String partitionKey = null;
					if (servId.equals(serviceIDFdr001)){
						String utcCreated = LocalDateTime.parse(reEvent.get(columnCreated).toString()).atZone(ZoneId.of("Europe/Rome")).toInstant().toString();
						partitionKey = utcCreated.substring(0,10);
						reEvent.put(columnCreated,utcCreated);
					} else {
						partitionKey = reEvent.get(columnCreated).toString().substring(0,10);
					}
					reEvent.put(partitionKeyColumnCreated,partitionKey);
					properties[index].forEach((p,v)->{
						String s = replaceDashWithUppercase(p);
						reEvent.put(s,v);
					});
					reEvent.put("timestamp",ZonedDateTime.now().toInstant().toEpochMilli());

					logger.log(Level.INFO, () -> String.format("Performing event ingestion: InvocationId [%s], Retry Attempt [%d], Events: %s", context.getInvocationId(), retryIndex, reEvents));

					toTableStorage(tableClient,new LinkedHashMap<>(reEvent));
					collection.insertOne(new Document(reEvent));
				}

				logger.log(Level.FINE, () -> "Done processing events");
            } else {
				isPersistenceOk = false;
				errorCause = String.format("[ALERT][FdrREToDS] AppException - Error processing events, lengths do not match: [events: %d - properties: %d]", reEvents.size(), properties.length);
            }
		} catch (NullPointerException e) {
			isPersistenceOk = false;
			errorCause = "[ALERT][FdrREToDS] AppException - Null pointer exception on fdr-re-events msg ingestion at " + LocalDateTime.now() + " : " + e;
        } catch (Exception e) {
			isPersistenceOk = false;
			errorCause = "[ALERT][FdrREToDS] AppException - Generic exception on fdr-re-events msg ingestion at " + LocalDateTime.now() + " : " + e.getMessage();
        }

		if (!isPersistenceOk) {
			String finalErrorCause = errorCause;
			logger.log(Level.SEVERE, () -> finalErrorCause);
			throw new AppException(errorCause);
		}
    }
}
