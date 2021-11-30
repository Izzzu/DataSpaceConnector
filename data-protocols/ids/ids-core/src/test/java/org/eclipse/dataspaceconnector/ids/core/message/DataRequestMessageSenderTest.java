package org.eclipse.dataspaceconnector.ids.core.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.ArtifactRequestMessageBuilder;
import de.fraunhofer.iais.eis.DynamicAttributeTokenBuilder;
import okhttp3.*;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.eclipse.dataspaceconnector.spi.iam.IdentityService;
import org.eclipse.dataspaceconnector.spi.iam.TokenResult;
import org.eclipse.dataspaceconnector.spi.monitor.Monitor;
import org.eclipse.dataspaceconnector.spi.security.Vault;
import org.eclipse.dataspaceconnector.spi.transfer.store.TransferProcessStore;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.DataAddress;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.DataRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.eclipse.dataspaceconnector.ids.spi.Protocols.IDS_REST;

class DataRequestMessageSenderTest {

    public static final String DESTINATION_KEY = "dataspaceconnector-data-destination";
    public static final String PROPERTIES_KEY = "dataspaceconnector-properties";
    static Faker faker = new Faker();
    private final String connectorAddress = "http://"+faker.internet().url();
    private final String connectorId = faker.internet().uuid();
    private final String processId = faker.internet().uuid();
    private final String assetId = faker.internet().url();
    private final ObjectMapper mapper = new ObjectMapper();


    private DataRequestMessageSender sender;
    private OkHttpClient httpClient;

    @BeforeEach
    public void setUp() {
        httpClient = niceMock(OkHttpClient.class);


        IdentityService identityService = niceMock(IdentityService.class);
        TokenResult tokenResult = niceMock(TokenResult.class);
        expect(tokenResult.getToken()).andReturn(faker.lorem().characters());
        expect(identityService.obtainClientCredentials(connectorId)).andReturn(tokenResult);
        replay(identityService, tokenResult);
        EasyMock.capture()
        sender = new DataRequestMessageSender("connectorId", identityService, niceMock(TransferProcessStore.class), EasyMock.mock(Vault.class), httpClient, mapper, niceMock(Monitor.class));

    }

    @Test
    public void initiateIDSMessage() {
        var additionalProperties = Map.of(faker.internet().uuid(), faker.lorem().word(), faker.internet().uuid(), faker.lorem().word());

        String type = faker.lorem().word();
        String secretName = faker.lorem().word();
        Map<String, Object> destinationMap = Map.of("type", type, "keyName", secretName, "properties", new HashMap<>());
        var dataRequest = DataRequest.Builder.newInstance()
                .connectorId(connectorId)
                .assetId(assetId)
                .dataDestination(DataAddress.Builder.newInstance().keyName(secretName).type(type).build())
                .protocol(IDS_REST)
                .additionalProperties(additionalProperties)
                .connectorAddress(connectorAddress)
                .build();

        expect(httpClient.newCall(matchRequest(additionalProperties, destinationMap))).andReturn(niceMock(Call.class)).times(1);
        replay(httpClient);

        // invoke
        sender.send(dataRequest, () -> processId);

        //verify
        verify(httpClient);


    }

    public Request matchRequest(Map<String, String> additionalProperties, Map<String, Object> destinationMap) {
        EasyMock.reportMatcher(new IArgumentMatcher() {
            @Override
            public boolean matches(Object argument) {
                if (!(argument instanceof Request)) return false;
                Map<String, Object> properties = mapper.convertValue(((Request) argument).body(), ArtifactRequestMessage.class).getProperties();
                return properties.get(DESTINATION_KEY).equals(destinationMap) &&
                        properties.get(PROPERTIES_KEY).equals(additionalProperties);
            }

            @Override
            public void appendTo(StringBuffer buffer) {
                buffer.append("OkHttp Requests don't match.");
            }
        });
        return null;
    }

    @NotNull
    private ArtifactRequestMessage createArtifactRequestMessage(Map<String, String> additionalProperties, Map<String, Serializable> destinationMap) {
        ArtifactRequestMessage artifactRequestMessage = new ArtifactRequestMessageBuilder()
                ._securityToken_(new DynamicAttributeTokenBuilder()
                        ._tokenValue_(faker.lorem().word())
                        .build())
                ._requestedArtifact_(URI.create(assetId))
                .build();

        artifactRequestMessage.setProperty(DESTINATION_KEY, destinationMap);
        artifactRequestMessage.setProperty(PROPERTIES_KEY, additionalProperties);
        return artifactRequestMessage;
    }

}