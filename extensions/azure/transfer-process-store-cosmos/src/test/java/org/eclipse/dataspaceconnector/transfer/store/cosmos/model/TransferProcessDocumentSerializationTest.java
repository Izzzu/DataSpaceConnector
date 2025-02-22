/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package org.eclipse.dataspaceconnector.transfer.store.cosmos.model;

import org.eclipse.dataspaceconnector.spi.types.TypeManager;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcess;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.dataspaceconnector.transfer.store.cosmos.TestHelper.createDataRequest;
import static org.eclipse.dataspaceconnector.transfer.store.cosmos.TestHelper.createManifest;
import static org.eclipse.dataspaceconnector.transfer.store.cosmos.TestHelper.createTransferProcess;

class TransferProcessDocumentSerializationTest {

    private TypeManager typeManager;

    @BeforeEach
    void setup() {
        typeManager = new TypeManager();
        typeManager.registerTypes(TransferProcessDocument.class, TransferProcess.class);
    }

    @Test
    void testSerialization() {

        var transferProcess = createTransferProcess();

        var document = TransferProcessDocument.from(transferProcess, "test-process");

        String s = typeManager.writeValueAsString(document);

        assertThat(s).isNotNull();
        assertThat(s).contains("\"partitionKey\":\"test-process\""); //should use the process id as partition key
        assertThat(s).contains("\"id\":\"test-process\"");
        assertThat(s).contains("\"id\":\"test-process\"");
        assertThat(s).contains("\"type\":\"CONSUMER\"");
        assertThat(s).contains("\"errorDetail\":null");
        assertThat(s).contains("\"destinationType\":\"Test Address Type\"");
        assertThat(s).contains("\"keyName\":\"Test Key Name\"");
        assertThat(s).contains("\"type\":\"Test Address Type\"");
    }

    @Test
    void testDeserialization() {
        var transferProcess = TransferProcess.Builder.newInstance()
                .id("test-process")
                .type(TransferProcess.Type.CONSUMER)
                .dataRequest(createDataRequest())
                .resourceManifest(createManifest())
                .build();

        var document = TransferProcessDocument.from(transferProcess, "test-process");
        String json = typeManager.writeValueAsString(document);

        var transferProcessDeserialized = typeManager.readValue(json, TransferProcessDocument.class);
        assertThat(transferProcessDeserialized).usingRecursiveComparison().isEqualTo(document);

    }


}
