package org.eclipse.dataspaceconnector.cosmos.azure;

import com.azure.cosmos.models.SqlQuerySpec;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public interface CosmosDbApi {

    void saveItem(CosmosDocument<?> item);

    @Nullable Object queryItemById(String id);

    @Nullable Object queryItemById(String id, String partitionKey);

    List<Object> queryAllItems(String partitionKey);

    List<Object> queryAllItems();

    Stream<Object> queryItems(SqlQuerySpec querySpec);

    Stream<Object> queryItems(String query);

    void deleteItem(String id);

    void createItems(Collection<CosmosDocument<?>> definitions);

    <T> String invokeStoredProcedure(String procedureName, String partitionKey, Object... args);
}
