/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Namespaceable;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.misc.archivable.impl.ArchivableEvents;

import java.io.IOException;

/**
 * ObjectsArchiver is the contract used by {@link ArchivableEvents} when handling archiving of {@link Objects}
 */
public interface ObjectsArchiver extends Namespaceable {
    /**
     * Will retrieve and archive a specific object by key.
     */
    default void archive(Objects objects, String namespace, String key) throws IOException {
        archive(objects, namespace);
    }

    /**
     * Will retrieve and archive all objects in the provided namespace.
     */
    void archive(Objects objects, String namespace) throws IOException;

    /**
     * Will restore an archived object for this namespace by key.
     * <br><br>
     * {@code restore()} with a key is not guaranteed to restore only the target object. It may restore up to the entire
     * rest of the archived namespace.
     * <br><br>
     * It will depend on the implementation
     */
    default void restore(Objects objects, String namespace, String key) throws IOException {
        restore(objects, namespace);
    }

    /**
     * Will restore all archived objects for this namespace.
     */
    void restore(Objects objects, String namespace) throws IOException;
}
