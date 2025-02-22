package org.eclipse.dataspaceconnector.cosmos.azure;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import org.eclipse.dataspaceconnector.common.string.StringUtils;
import org.eclipse.dataspaceconnector.spi.EdcException;
import org.eclipse.dataspaceconnector.spi.security.Vault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CosmosDbApiImpl implements CosmosDbApi {

    private static final String HOST_TEMPLATE = "https://%s.documents.azure.com:443/";

    private final CosmosItemRequestOptions itemRequestOptions;
    private final CosmosQueryRequestOptions queryRequestOptions;
    private final CosmosContainer container;

    public CosmosDbApiImpl(@NotNull CosmosContainer container, boolean isQueryMetricsEnabled) {
        queryRequestOptions = new CosmosQueryRequestOptions();
        queryRequestOptions.setQueryMetricsEnabled(isQueryMetricsEnabled);
        itemRequestOptions = new CosmosItemRequestOptions();

        this.container = container;
    }

    public CosmosDbApiImpl(@NotNull Vault vault, @NotNull AbstractCosmosConfig config) {
        this(getContainer(vault, config), config.isQueryMetricsEnabled());
    }

    private static void handleResponse(CosmosItemResponse<?> response) {
        int code = response.getStatusCode();
        if (code < 200 || code >= 300) {
            throw new EdcException("Error during CosmosDB interaction: " + code);
        }
    }

    private static CosmosContainer getContainer(Vault vault, AbstractCosmosConfig config) {
        CosmosClient client = createClient(vault, config.getAccountName(), Collections.singletonList(config.getPreferredRegion()));
        CosmosDatabase database = getDatabase(client, config.getDbName());
        if (database.readAllContainers().stream().noneMatch(sp -> sp.getId().equals(config.getContainerName()))) {
            throw new EdcException("No CosmosDB container named '" + config.getContainerName() + "' was found in account '" + config.getAccountName() + "'. Please create one, preferably using terraform.");
        }
        return database.getContainer(config.getContainerName());
    }

    private static CosmosClient createClient(Vault vault, String accountName, List<String> preferredRegions) {
        var accountKey = vault.resolveSecret(accountName);
        if (StringUtils.isNullOrEmpty(accountKey)) {
            throw new EdcException("No credentials found in vault for Cosmos DB '" + accountName + "'");
        }

        // create cosmos db api client
        String host = String.format(HOST_TEMPLATE, accountName);
        return new CosmosClientBuilder()
                .endpoint(host)
                .key(accountKey)
                .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();
    }

    private static CosmosDatabase getDatabase(CosmosClient client, String databaseName) {
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        return client.getDatabase(databaseResponse.getProperties().getId());
    }

    @Override
    public void saveItem(CosmosDocument<?> item) {
        try {
            // we don't need to supply a partition key, it will be extracted from the CosmosDocument
            CosmosItemResponse<Object> response = container.upsertItem(item, itemRequestOptions);
            handleResponse(response);
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public @Nullable Object queryItemById(String id) {
        var query = new SqlQuerySpec("SELECT * FROM c WHERE c.id = @id", new SqlParameter("@id", id));

        try {
            var list = container.queryItems(query, queryRequestOptions, Object.class).stream().collect(Collectors.toList());
            return list.isEmpty() ? null : list.get(0);
        } catch (CosmosException e) {
            throw new EdcException(e);
        }

    }

    @Override
    public @Nullable Object queryItemById(String id, String partitionKey) {
        CosmosItemResponse<Object> response;
        try {
            response = container.readItem(id, new PartitionKey(partitionKey), itemRequestOptions, Object.class);
        } catch (NotFoundException e) {
            return null;
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
        handleResponse(response);
        return response.getItem();
    }

    @Override
    public List<Object> queryAllItems(String partitionKey) {
        try {
            return container.readAllItems(new PartitionKey(partitionKey), queryRequestOptions, Object.class).stream().collect(Collectors.toList());
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public List<Object> queryAllItems() {
        var query = new SqlQuerySpec("SELECT * FROM c");
        try {
            return container.queryItems(query, queryRequestOptions, Object.class).stream().collect(Collectors.toList());
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public Stream<Object> queryItems(SqlQuerySpec querySpec) {
        try {
            return container.queryItems(querySpec, queryRequestOptions, Object.class).stream();
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public Stream<Object> queryItems(String query) {
        try {
            return container.queryItems(query, queryRequestOptions, Object.class).stream();
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public void deleteItem(String id) {

        // we need to query the item first, because delete-by-id requires a partition key, which we might not have available here
        var item = queryItemById(id);
        if (item == null) {
            throw new NotFoundException("An object with the ID " + id + " could not be found!");
        }
        try {
            container.deleteItem(item, itemRequestOptions).getItem();
        } catch (CosmosException e) {
            throw new EdcException(e);
        }
    }

    @Override
    public void createItems(Collection<CosmosDocument<?>> definitions) {
        definitions.forEach(this::saveItem);
    }

    @Override
    public <T> String invokeStoredProcedure(String procedureName, String partitionKey, Object... args) {
        var sproc = getStoredProcedure(procedureName);

        List<Object> params = Arrays.asList(args);
        var options = new CosmosStoredProcedureRequestOptions();
        options.setPartitionKey(new PartitionKey(partitionKey));

        CosmosStoredProcedureResponse response = sproc.execute(params, options);
        return response.getResponseAsString();
    }


    private CosmosStoredProcedure getStoredProcedure(String sprocName) {
        return container.getScripts().getStoredProcedure(sprocName);
    }


}
