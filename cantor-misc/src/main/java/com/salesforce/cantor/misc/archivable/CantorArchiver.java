/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable;

/**
 * Abstraction for accessing all underlying archives
 */
public interface CantorArchiver {

    /**
     * Returns an instance of SetsArchiver.
     *
     * @return instance of sets archiver
     */
    SetsArchiver sets();

    /**
     * Returns an instance of objectsArchiver.
     *
     * @return instance of objects archiver
     */
    ObjectsArchiver objects();

    /**
     * Returns an instance of EventsArchiver.
     *
     * @return instance of events archiver
     */
    EventsArchiver events();
}
