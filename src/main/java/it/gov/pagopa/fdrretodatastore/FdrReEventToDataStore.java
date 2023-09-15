package it.gov.pagopa.fdrretodatastore;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import it.gov.pagopa.fdrretodatastore.util.ObjectMapperUtils;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

	private Pattern replaceDashPattern = Pattern.compile("-([a-zA-Z])");
	private static String idField = "uniqueId";
	private static String tableName = System.getenv("TABLE_STORAGE_TABLE_NAME");
	private static String columnCreated = "created";
	private static String partitionKeyColumnCreated = "PartitionKey";

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


	private void toTableStorage(Logger logger,TableClient tableClient,Map<String,Object> reEvent) throws JsonProcessingException {
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
    public void processNodoReEvent (
            @EventHubTrigger(
                    name = "FdrReEvent",
                    eventHubName = "", // blank because the value is included in the connection string
                    connection = "EVENTHUB_CONN_STRING",
                    cardinality = Cardinality.MANY)
    		List<Map<String,Object>> reEvents,
    		@BindingName(value = "PropertiesArray") Map<String, Object>[] properties,
            final ExecutionContext context) {

		Logger logger = context.getLogger();

		MongoDatabase database = getMongoClient().getDatabase(System.getenv("COSMOS_DB_NAME"));
		MongoCollection<Document> collection = database.getCollection(System.getenv("COSMOS_DB_COLLECTION_NAME"));

		TableClient tableClient = getTableServiceClient().getTableClient(tableName);

		String msg = String.format("Persisting %d events",reEvents.size());
		logger.info(msg);

        try {
        	if (reEvents.size() == properties.length) {
				for(int index=0;index< properties.length;index++){
					logger.info("processing "+(index+1)+" of "+properties.length);
//					final Map<String,Object> reEvent = ObjectMapperUtils.readValue(reEvents.get(index), Map.class);
					final Map<String,Object> reEvent = reEvents.get(index);
					String partitionKey = reEvent.get(columnCreated).toString().substring(0,10);
					reEvent.put(partitionKeyColumnCreated,partitionKey);
					properties[index].forEach((p,v)->{
						String s = replaceDashWithUppercase(p);
						reEvent.put(s,v);
					});
					reEvent.put("timestamp",ZonedDateTime.now().toInstant().toEpochMilli());

					toTableStorage(logger,tableClient,new LinkedHashMap<>(reEvent));
					collection.insertOne(new Document(reEvent));

				}
				logger.info("Done processing events");
            } else {
				logger.severe("Error processing events, lengths do not match ["+reEvents.size()+","+properties.length+"]");
            }
		} catch (NullPointerException e) {
            logger.severe("NullPointerException exception on cosmos fdr-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        } catch (Exception e) {
            logger.severe("Generic exception on cosmos fdr-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        }

    }
}
