/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.functions.executors;

import com.salesforce.cantor.functions.Context;
import com.salesforce.cantor.functions.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class BundleExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(BundleExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Collections.singletonList("bundle");
    }

    @Override
    public void run(final String function,
                    final byte[] body,
                    final Context context,
                    final Map<String, String> params) throws IOException {
        // TODO
        // body will be the bytes representation of the compressed bundle object
        // to run, decompress bytes, store them in a temporary location
        // expect a parameter called '.script' to be present in 'params'
        // invoke the script in bundle with the given name
        // read stdout and stderr from process builder and do `context.set("stdout", out)` and `context.set("stderr", err)`
    }
}
