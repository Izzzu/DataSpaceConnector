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
 *       Fraunhofer Institute for Software and Systems Engineering - adapt class for negotiation process
 *
 */

package org.eclipse.dataspaceconnector.contract.negotiation;

import org.eclipse.dataspaceconnector.spi.contract.negotiation.NegotiationWaitStrategy;

/**
 * Implements an exponential backoff strategy for failed iterations.
 * @// FIXME: 30.11.2021 Duplicate of class in transfer extension, move class to other package to prevent dependencies between extensions.
 */
public class ExponentialWaitStrategy implements NegotiationWaitStrategy {
    private final long successWaitPeriodMillis;
    private int errorCount = 0;


    public ExponentialWaitStrategy(long successWaitPeriodMillis) {
        this.successWaitPeriodMillis = successWaitPeriodMillis;
    }

    @Override
    public long waitForMillis() {
        return successWaitPeriodMillis;
    }

    @Override
    public void success() {
        errorCount = 0;
    }

    @Override
    public long retryInMillis() {
        errorCount++;
        double exponentialMultiplier = Math.pow(2.0, errorCount - 1);
        double result = exponentialMultiplier * successWaitPeriodMillis;
        return (long) Math.min(result, Long.MAX_VALUE);
    }
}
