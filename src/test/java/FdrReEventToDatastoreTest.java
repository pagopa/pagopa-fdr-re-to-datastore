import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.RetryContext;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import it.gov.pagopa.fdrretodatastore.FdrReEventToDataStore;
import it.gov.pagopa.fdrretodatastore.exception.AppException;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.powermock.reflect.Whitebox;
import util.TestUtil;

import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FdrReEventToDatastoreTest {

    @Spy
    FdrReEventToDataStore fdrReEventToDataStore;

    @Mock
    ExecutionContext context;

    private static final Logger logger = Logger.getLogger("FdrReEventToDataStore-test-logger");

    @Test
    @SneakyThrows
    void runOk_eventFDR001() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        TableClient tableClient = mock(TableClient.class);
        when(tableServiceClient.getTableClient(anyString())).thenReturn(tableClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient",  tableServiceClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName",  "errors");

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        String eventJson = TestUtil.readStringFromFile("events/event_FDR001_ok.json");
        String eventFDR001Ok = String.format(eventJson, LocalDateTime.now(), UUID.randomUUID());

        // generating input
        assertDoesNotThrow(() ->
                fdrReEventToDataStore.processNodoReEvent(List.of(eventFDR001Ok), new HashMap[]{new HashMap()}, context));
    }

    @Test
    @SneakyThrows
    void runOk_eventFDR001_2() {
        when(context.getLogger()).thenReturn(logger);
        RetryContext retryContext = mock(RetryContext.class);
        when(context.getRetryContext()).thenReturn(retryContext);
        when(retryContext.getRetrycount()).thenReturn(2);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        TableClient tableClient = mock(TableClient.class);
        when(tableServiceClient.getTableClient(anyString())).thenReturn(tableClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient",  tableServiceClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName",  "errors");

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        String eventJson = TestUtil.readStringFromFile("events/event_FDR001_ok.json");
        String eventFDR001Ok = String.format(eventJson, LocalDateTime.now(), UUID.randomUUID());

        // generating input
        assertDoesNotThrow(() ->
                fdrReEventToDataStore.processNodoReEvent(List.of(eventFDR001Ok), new HashMap[]{new HashMap()}, context));
    }

    @Test
    @SneakyThrows
    void runOk_eventFDR003() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        TableClient tableClient = mock(TableClient.class);
        when(tableServiceClient.getTableClient(anyString())).thenReturn(tableClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient",  tableServiceClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName",  "errors");

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        String eventJson = TestUtil.readStringFromFile("events/event_FDR003_ok.json");
        String eventFDR003Ok = String.format(eventJson, LocalDateTime.now(), UUID.randomUUID());

        List<Map<String, Object>> propertiesList = new ArrayList<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put("prop1", "1");
        properties.put("prop2", "2");
        propertiesList.add(properties);

        // generating input
        Map<String,Object>[] propertiesArray = new HashMap[propertiesList.size()];
        propertiesArray = propertiesList.toArray(propertiesArray);
        Map<String, Object>[] finalPropertiesArray = propertiesArray;

        assertDoesNotThrow(() ->
                fdrReEventToDataStore.processNodoReEvent(List.of(eventFDR003Ok), finalPropertiesArray, context));
    }

    @Test
    @SneakyThrows
    void runOk_eventFDR003_2() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        TableClient tableClient = mock(TableClient.class);
        when(tableServiceClient.getTableClient(anyString())).thenReturn(tableClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient",  tableServiceClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName",  "errors");

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        String eventJson = TestUtil.readStringFromFile("events/event_FDR003_ok.json");
        String eventFDR003Ok = String.format(eventJson, LocalDateTime.now(), UUID.randomUUID());

        List<Map<String, Object>> propertiesList = new ArrayList<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put("prop1-"+UUID.randomUUID(), "1");
        properties.put("prop2", "2");
        propertiesList.add(properties);

        // generating input
        Map<String,Object>[] propertiesArray = new HashMap[propertiesList.size()];
        propertiesArray = propertiesList.toArray(propertiesArray);
        Map<String, Object>[] finalPropertiesArray = propertiesArray;

        assertDoesNotThrow(() ->
                fdrReEventToDataStore.processNodoReEvent(List.of(eventFDR003Ok), finalPropertiesArray, context));
    }

    @Test
    @SneakyThrows
    void runKo_noEvents() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        TableClient tableClient = mock(TableClient.class);
        when(tableServiceClient.getTableClient(anyString())).thenReturn(tableClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient",  tableServiceClient);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName",  "errors");

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        // generating input
        assertThrows(AppException.class, () ->
                fdrReEventToDataStore.processNodoReEvent(Collections.emptyList(), new HashMap[]{new HashMap()}, context));
    }

    @Test
    @SneakyThrows
    void runKo_NullPointException() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient", tableServiceClient);

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClient", mongoClient);

        // generating input
        assertThrows(AppException.class, () ->
                fdrReEventToDataStore.processNodoReEvent(Collections.emptyList(), null, context));
    }

    @Test
    @SneakyThrows
    void runKo_GenericException() {
        when(context.getLogger()).thenReturn(logger);

        Whitebox.setInternalState(FdrReEventToDataStore.class, "MAX_RETRY_COUNT", -1);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoClientURI", "mongodb://localhost:8080");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoDatabaseName", "mongoDatabaseName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "mongoCollectionName", "mongoCollectionName");
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableName", "errors");

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        Whitebox.setInternalState(FdrReEventToDataStore.class, "tableServiceClient", tableServiceClient);

        String eventJson = TestUtil.readStringFromFile("events/event_FDR003_malformed.json");
        String eventFDR003Ok = String.format(eventJson, LocalDateTime.now(), UUID.randomUUID());

        List<Map<String, Object>> propertiesList = new ArrayList<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put("prop1-"+UUID.randomUUID(), "1");
        properties.put("prop2", "2");
        propertiesList.add(properties);

        // generating input
        Map<String,Object>[] propertiesArray = new HashMap[propertiesList.size()];
        propertiesArray = propertiesList.toArray(propertiesArray);
        Map<String, Object>[] finalPropertiesArray = propertiesArray;

        assertThrows(AppException.class, () ->
                fdrReEventToDataStore.processNodoReEvent(List.of(eventFDR003Ok), finalPropertiesArray, context));
    }

}
