/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

import com.google.gson.Gson;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.functions.Functions;
import com.salesforce.cantor.functions.FunctionsOnCantor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.*;

@Component
@Path("/functions")
@Tag(name = "Functions Resource", description = "Api for handling Cantor functions")
public class FunctionsResource {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();

    private final Cantor cantor;
    private final Functions functions;

    @Autowired
    public FunctionsResource(final Cantor cantor) {
        this.cantor = cantor;
        this.functions = new FunctionsOnCantor(cantor);
        try {
            this.functions.create("rca");
            this.functions.store("rca", "pidstat.groovy", "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "static String normalize(String name) {\n" +
                    "    Map patternToName = [\n" +
                    "    \".*pool.*\": \"unnamed-thread-pool\",\n" +
                    "            \".*lettuce.*\": \"lettuce\",\n" +
                    "            \".*aura.*\": \"aura\",\n" +
                    "            \".*qtp.*\": \"qtp\",\n" +
                    "            \".*apex.*\": \"apex\",\n" +
                    "            \".*mockrequest.*\": \"mock-request\",\n" +
                    "            \".*gcthread.*\": \"gc-thread\",\n" +
                    "            \".*kafka.*\": \"kafka\",\n" +
                    "            \".*solr.*\": \"solr\",\n" +
                    "            \".*search.*\": \"search\",\n" +
                    "            \".*sfdcpinger.*\": \"sfdc-pinger\",\n" +
                    "            \".*vmthread.*\": \"vm-thread\",\n" +
                    "            \".*metricpub.*\": \"metrics-publishing\",\n" +
                    "            \".*wardenagent.*\": \"warden-agent\"\n" +
                    "    ]\n" +
                    "\n" +
                    "    name = name.toLowerCase().replaceAll(\"[^a-z]\", \"\")\n" +
                    "    for (Map.Entry entry : patternToName.entrySet()) {\n" +
                    "        if (name.toLowerCase().matches(entry.key)) {\n" +
                    "            return entry.value\n" +
                    "        }\n" +
                    "    }\n" +
                    "    return name\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = \"maiev-tenant-\" + getRequiredParameter(parameters, \"tenant\")\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "Map processToCpu = new HashMap()\n" +
                    "double totalCpuUsage = 0\n" +
                    "Map systmCpu =new HashMap()\n" +
                    "double totalSystemCpuUsage = 0\n" +
                    "Map userCpu =new HashMap()\n" +
                    "double totalUserCpuUsage = 0\n" +
                    "for (long i = start; i < end; i += TimeUnit.HOURS.toMillis(1)) {\n" +
                    "    long j = Math.min(i + TimeUnit.HOURS.toMillis(1), end)\n" +
                    "    Set<String> instances = cx.cantor.events().metadata(namespace, \"instance-id\", i, j, null, null)\n" +
                    "    for (String instance : instances) {\n" +
                    "        // get events with compressed payloads\n" +
                    "        List<Events.Event> compressedPayloads = cx.cantor.events().get(namespace, i, j, [\"name\":\"pidstat\", \"instance-id\": instance as String], null, true)\n" +
                    "        List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "        // go through events one by one and decompress the payload\n" +
                    "        for (Events.Event e : compressedPayloads) {\n" +
                    "            events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "        }\n" +
                    "        for (Events.Event e : events) {\n" +
                    "            String payload = new String(e.payload)\n" +
                    "            List lines = payload.split(\"\\n\")\n" +
                    "            for (String line : lines) {\n" +
                    "                String[] words = line.trim().replaceAll(\"\\\\s+\", \" \").split(\" \")\n" +
                    "                if (!words[0].matches(\"-?\\\\d+(\\\\.\\\\d+)?\")) continue\n" +
                    "                double cpuUsage = words[7] as double\n" +
                    "                double usrCpu = words[4] as double\n" +
                    "                double systemCpu = words[5] as double\n" +
                    "                if (cpuUsage > 0.0 && cpuUsage < 100.0) {\n" +
                    "                    String processName = normalize(words[19..-1].join(\" \"))\n" +
                    "                    if (!processToCpu.containsKey(processName)) {\n" +
                    "                        processToCpu.put(processName, 0.0)\n" +
                    "                    }\n" +
                    "                    if (!systmCpu.containsKey(processName)) {\n" +
                    "                        systmCpu.put(processName, 0.0)\n" +
                    "                    }\n" +
                    "                    if (!userCpu.containsKey(processName)) {\n" +
                    "                        userCpu.put(processName, 0.0)\n" +
                    "                    }\n" +
                    "                    processToCpu.put(processName, cpuUsage + processToCpu.get(processName))\n" +
                    "                    totalCpuUsage += cpuUsage\n" +
                    "                    systmCpu.put(processName, systemCpu + systmCpu.get(processName))\n" +
                    "                    totalSystemCpuUsage += systemCpu\n" +
                    "                    userCpu.put(processName, usrCpu + userCpu.get(processName))\n" +
                    "                    totalUserCpuUsage += usrCpu\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n" +
                    "// sort by value\n" +
                    "Map sorted = processToCpu.sort { -it.value }\n" +
                    "Map sortedSystmCpu = systmCpu.sort { -it.value }\n" +
                    "Map sortedUsrCpu = userCpu.sort { -it.value }\n" +
                    "Map results = new LinkedHashMap()\n" +
                    "Map resultsSystmCpu =new LinkedHashMap()\n" +
                    "Map resultsUsrCpu = new LinkedHashMap()\n" +
                    "int count = 0;\n" +
                    "int countSysCpu = 0;\n" +
                    "int countUsrCpu = 0;\n" +
                    "for (entry in sorted) {\n" +
                    "        results.put(entry.key, entry.value)\n" +
                    "        if (count++ == 100) break\n" +
                    "    }\n" +
                    "for (entry in sortedSystmCpu) {\n" +
                    "        resultsSystmCpu.put(entry.key, entry.value)\n" +
                    "        if (countSysCpu++ == 20) break\n" +
                    "    }\n" +
                    "for (entry in sortedUsrCpu) {\n" +
                    "        resultsUsrCpu.put(entry.key, entry.value)\n" +
                    "        if (countUsrCpu++ == 50) break\n" +
                    "}\n" +
                    "String output = \"CPU Accumulated          Process Name\\n\"\n" +
                    "output +=       \"-------------------------------------\\n\"\n" +
                    "        for (entry in results) {\n" +
                    "    output += (entry.value / totalCpuUsage) + \"  \\t---->\\t \" + entry.key + \"\\n\"\n" +
                    "}\n" +
                    "String outputSystmCpu = \"System CPU Accumulated          Process Name\\n\"\n" +
                    "outputSystmCpu +=       \"-------------------------------------\\n\"\n" +
                    "        for (entry in resultsSystmCpu) {\n" +
                    "    outputSystmCpu += (entry.value / totalSystemCpuUsage) + \"  \\t---->\\t \" + entry.key + \"\\n\"\n" +
                    "}\n" +
                    "String outputUserCpu = \"User CPU Accumulated          Process Name\\n\"\n" +
                    "outputUserCpu +=       \"-------------------------------------\\n\"\n" +
                    "        for (entry in resultsUsrCpu) {\n" +
                    "    outputUserCpu += (entry.value / totalUserCpuUsage) + \"  \\t---->\\t \" + entry.key + \"\\n\"\n" +
                    "}\n" +
                    "// context.set(\".out\", results.sort { -it.value })\n" +
                    "context.set(\".out\", output + \"\\n\\n\\n---\\n\\n\\n\" + outputSystmCpu + \"\\n\\n\\n---\\n\\n\\n\" + outputUserCpu)\n");
            this.functions.store("rca", "cpu-gov-check.groovy", "// run it like this: /maiev/cpu-gov-check.groovy\n" +
                    "// it would go through the past 1 hour data and find hosts that have cpu-governor set to powersave\n" +
                    "\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "Context cx = context\n" +
                    "\n" +
                    "String out = \"\"\n" +
                    "boolean foundAny = false\n" +
                    "for (String n : cx.cantor.events().namespaces()) {\n" +
                    "    try {\n" +
                    "        long first = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)\n" +
                    "        long last = System.currentTimeMillis()\n" +
                    "        Set<String> instances = cx.cantor.events().metadata(n, \"instance-id\", first, last, null, null)\n" +
                    "        for (String instance : instances) {\n" +
                    "            // get events with compressed payloads\n" +
                    "            List<Events.Event> compressedPayloads = cx.cantor.events().get(n, first, last, [\"name\": \"system-cpu-scaling-governor\", \"instance-id\": instance as String], null, true)\n" +
                    "            List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "            // go through events one by one and decompress the payload\n" +
                    "            for (Events.Event e : compressedPayloads) {\n" +
                    "                events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "            }\n" +
                    "\n" +
                    "            for (Events.Event e : events) {\n" +
                    "                String payload = new String(e.payload)\n" +
                    "                if (payload.contains(\"powersave\")) {\n" +
                    "                    foundAny = true\n" +
                    "                    out += \"host: \" + e.metadata.get(\"host\") + payload + \"\\n\"\n" +
                    "                } else {\n" +
                    "                    // uncomment for debugging\n" +
                    "                    // out += \".\\n\"\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    } catch (Exception e) {\n" +
                    "        // uncomment for debugging\n" +
                    "        // out += \"failed for \" + n + \" with error: \" + e.message + \"\\n\"\n" +
                    "    }\n" +
                    "}\n" +
                    "if (!foundAny) {\n" +
                    "    out = \"did not find any host in powersave mode\"\n" +
                    "}\n" +
                    "\n" +
                    "cx.set(\".out\", out)");
            this.functions.store("rca", "top-cpu-consuming-stack-trace.groovy", "// get pidstat, find top thread, lookup in jstack\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Cantor\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import com.salesforce.cantor.grpc.CantorOnGrpc\n" +
                    "\n" +
                    "import java.text.DateFormat\n" +
                    "import java.text.SimpleDateFormat\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "static List<Events.Event> getEvents(Cantor cantor, String namespace, long start, long end, Map mq, Map dq, boolean includePayloads) {\n" +
                    "    List<Events.Event> results = cantor.events().get(namespace, start, end, mq, dq, includePayloads)\n" +
                    "    if (!includePayloads) {\n" +
                    "        return results\n" +
                    "    }\n" +
                    "    List<Events.Event> decompressedResults = new ArrayList<>(results.size())\n" +
                    "    // go through events one by one and decompress the payload\n" +
                    "    for (Events.Event e : results) {\n" +
                    "        decompressedResults.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "    }\n" +
                    "    return decompressedResults\n" +
                    "}\n" +
                    "\n" +
                    "// find the stack for the given thread id in the jstack event\n" +
                    "String getStack(Events.Event jstack, long threadId) {\n" +
                    "    // convert thread id to hex and prefix with 'nid=0x'\n" +
                    "    String nidString = \" nid=0x\" + Long.toHexString(threadId) + \" \"\n" +
                    "    String payload = new String(jstack.payload)\n" +
                    "    for (String stack : payload.split(\"\\n\\n\")) {\n" +
                    "        if (stack.contains(nidString)) {\n" +
                    "            return stack\n" +
                    "        }\n" +
                    "    }\n" +
                    "    return \"stack not found for:\" + nidString\n" +
                    "}\n" +
                    "\n" +
                    "String toDateString(timestampMillis) {\n" +
                    "    DateFormat formatter = new SimpleDateFormat(\"YYYY-MM-dd hh:mm:ss\")\n" +
                    "    return formatter.format(new Date(Long.valueOf(timestampMillis)))\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = \"maiev-tenant-\" + getRequiredParameter(parameters, \"tenant\")\n" +
                    "\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "\n" +
                    "String output = \"Top CPU consuming stack report - namespace: \" + namespace\n" +
                    "output += \"\\n\\n-------------------------------------------------------------------------------------------------------------------------------------------------\\n\\n\"\n" +
                    "jstackCount = 0\n" +
                    "pidstatCount = 0\n" +
                    "instanceCount = 0\n" +
                    "for (int i = end; i < start; ++i) {\n" +
                    "    long first = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i)\n" +
                    "    long last = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i - 1)\n" +
                    "    Set<String> instances = cx.cantor.events().metadata(namespace, \"instance-id\", first, last, null, null)\n" +
                    "    instanceCount += instances.size()\n" +
                    "    for (String instance : instances) {\n" +
                    "        List<Events.Event> jstacks = getEvents(cx.cantor, namespace, first, last, [\"name\":\"jstack\", \"instance-id\": instance as String], null, true)\n" +
                    "        jstackCount += jstacks.size()\n" +
                    "        for (Events.Event jstack : jstacks) {\n" +
                    "            // find pidstats 30 seconds before and after the jstack\n" +
                    "            List<Events.Event> pidstats = getEvents(cx.cantor, namespace, jstack.timestampMillis - 120_000, jstack.timestampMillis + 120_000, [\"name\":\"pidstat\", \"instance-id\": instance as String], null, true)\n" +
                    "            pidstatCount += pidstats.size()\n" +
                    "            Map processToCpu = new HashMap()\n" +
                    "            for (Events.Event pidstat : pidstats) {\n" +
                    "                String payload = new String(pidstat.payload)\n" +
                    "                List lines = payload.split(\"\\n\")\n" +
                    "                for (String line : lines) {\n" +
                    "                    String[] words = line.trim().replaceAll(\"\\\\s+\", \" \").split(\" \")\n" +
                    "                    if (!words[0].matches(\"-?\\\\d+(\\\\.\\\\d+)?\")) continue\n" +
                    "                    double cpuUsage = words[7] as double\n" +
                    "                    if (cpuUsage > 0.0 && cpuUsage < 100.0) {\n" +
                    "                        // String processName = words[19..-1].join(\" \")\n" +
                    "                        String threadId = words[3]\n" +
                    "                        if (!processToCpu.containsKey(threadId)) {\n" +
                    "                            processToCpu.put(threadId, 0.0)\n" +
                    "                        }\n" +
                    "                        processToCpu.put(threadId, cpuUsage + processToCpu.get(threadId))\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            }\n" +
                    "            Map sorted = processToCpu.sort { -it.value }\n" +
                    "            int count = 0;\n" +
                    "            for (entry in sorted) {\n" +
                    "                if (count++ == 3) break\n" +
                    "                try {\n" +
                    "                String stack = getStack(jstack, entry.key as long)\n" +
                    "//                String fingerPrint = stack.replaceAll(/.*nid=.*\\n/, '').replaceAll(/.*at ([a-z\\\\.]+)\\\\.[A-Z]+.*/, '$1 --- ')\n" +
                    "                output += \"Instance: \" + instance + \"\\n\"\n" +
                    "                output += \"Timestamp: \" + toDateString(jstack.timestampMillis) + \"\\n\"\n" +
                    "                output += \"Accumulated CPU usage: \" + entry.value + \"%\\n\"\n" +
                    "                output += \"Thread ID: \" + entry.key + \"\\n\"\n" +
                    "//                output += \"Stack fingerprint: \" + fingerPrint + \"\\n\"\n" +
                    "                output += \"Stack trace:\\n\" + stack\n" +
                    "                output += \"\\n\\n-------------------------------------------------------------------------------------------------------------------------------------------------\\n\\n\"\n" +
                    "                } catch (Exception e) {\n" +
                    "                    output += e.message\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "output += \"\\n\\nTotal processed events: \" + instanceCount + \" instances, \" + jstackCount + \" jstacks, \" + pidstatCount + \" pidstats.\\n\"\n" +
                    "\n" +
                    "// context.set(\".out\", results.sort { -it.value })\n" +
                    "context.set(\".out\", output)");
            this.functions.store("rca", "pidstat-cpu-summary.groovy", "// http://localhost:8083/api/functions/run/maiev/pidstat-cpu-summary.groovy?start=4&end=0&tenant=gs0\n" +
                    "\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Cantor\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import com.salesforce.cantor.grpc.CantorOnGrpc\n" +
                    "\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = \"maiev-tenant-\" + getRequiredParameter(parameters, \"tenant\")\n" +
                    "\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "\n" +
                    "Map processToCpu = new HashMap()\n" +
                    "\n" +
                    "for (int i = end; i < start; ++i) {\n" +
                    "    long first = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i)\n" +
                    "    long last = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i - 1)\n" +
                    "    Set<String> instances = cx.cantor.events().metadata(namespace, \"instance-id\", first, last, null, null)\n" +
                    "    for (String instance : instances) {\n" +
                    "        // get events with compressed payloads\n" +
                    "        List<Events.Event> compressedPayloads = cx.cantor.events().get(namespace, first, last, [\"name\":\"pidstat\", \"instance-id\": instance as String], null, true)\n" +
                    "        List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "        // go through events one by one and decompress the payload\n" +
                    "        for (Events.Event e : compressedPayloads) {\n" +
                    "            events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "        }\n" +
                    "\n" +
                    "        for (Events.Event e : events) {\n" +
                    "            String payload = new String(e.payload)\n" +
                    "            List lines = payload.split(\"\\n\")\n" +
                    "            for (String line : lines) {\n" +
                    "                String[] words = line.trim().replaceAll(\"\\\\s+\", \" \").split(\" \")\n" +
                    "                if (!words[0].matches(\"-?\\\\d+(\\\\.\\\\d+)?\")) continue\n" +
                    "                double cpuUsage = words[7] as double\n" +
                    "                if (cpuUsage > 0.0 && cpuUsage < 100.0) {\n" +
                    "                    String processName = words[19..-1].join(\" \")\n" +
                    "                    if (!processToCpu.containsKey(processName)) {\n" +
                    "                        processToCpu.put(processName, 0.0)\n" +
                    "                    }\n" +
                    "                    processToCpu.put(processName, cpuUsage + processToCpu.get(processName))\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "// sort by value\n" +
                    "Map sorted = processToCpu.sort { -it.value }\n" +
                    "\n" +
                    "Map results = new LinkedHashMap()\n" +
                    "int count = 0;\n" +
                    "for (entry in sorted) {\n" +
                    "    results.put(entry.key, entry.value)\n" +
                    "    if (count++ == 100) break\n" +
                    "}\n" +
                    "\n" +
                    "String output = \"CPU Accumulated          Process Name\\n\"\n" +
                    "output +=       \"-------------------------------------\\n\"\n" +
                    "for (entry in results) {\n" +
                    "    output += (entry.value as int) + \"  \\t---->\\t \" + entry.key + \"\\n\"\n" +
                    "}\n" +
                    "\n" +
                    "// context.set(\".out\", results.sort { -it.value })\n" +
                    "context.set(\".out\", output)");
            this.functions.store("rca", "top-cpu-summary.groovy", "// http://localhost:8083/api/functions/run/maiev/top-cpu-summary.groovy?start=24&end=0&tenant=gs0\n" +
                    "\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Cantor\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import com.salesforce.cantor.grpc.CantorOnGrpc\n" +
                    "\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = \"maiev-tenant-\" + getRequiredParameter(parameters, \"tenant\")\n" +
                    "\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "\n" +
                    "Map processToCpu = new HashMap()\n" +
                    "\n" +
                    "for (int i = end; i < start; ++i) {\n" +
                    "    long first = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i)\n" +
                    "    long last = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(start + i - 1)\n" +
                    "    Set<String> instances = cx.cantor.events().metadata(namespace, \"instance-id\", first, last, null, null)\n" +
                    "    for (String instance : instances) {\n" +
                    "        // get events with compressed payloads\n" +
                    "        List<Events.Event> compressedPayloads = cx.cantor.events().get(namespace, first, last, [\"name\":\"top\", \"instance-id\": instance as String], null, true)\n" +
                    "        List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "        // go through events one by one and decompress the payload\n" +
                    "        for (Events.Event e : compressedPayloads) {\n" +
                    "            events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "        }\n" +
                    "\n" +
                    "        for (Events.Event e : events) {\n" +
                    "            String payload = new String(e.payload)\n" +
                    "            List lines = payload.split(\"\\n\")\n" +
                    "            for (String line : lines) {\n" +
                    "                String[] words = line.replaceAll(\"\\\\s+\", \" \").split(\" \")\n" +
                    "                if (!words[0].matches(\"-?\\\\d+(\\\\.\\\\d+)?\")) continue\n" +
                    "                double cpuUsage = words[8] as double\n" +
                    "                if (cpuUsage > 0.0) {\n" +
                    "                    String processName = words[11..-1].join(\" \")\n" +
                    "                    if (!processToCpu.containsKey(processName)) {\n" +
                    "                        processToCpu.put(processName, 0.0)\n" +
                    "                    }\n" +
                    "                    processToCpu.put(processName, cpuUsage + processToCpu.get(processName))\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "// sort by value\n" +
                    "Map sorted = processToCpu.sort { -it.value }\n" +
                    "\n" +
                    "Map results = new LinkedHashMap()\n" +
                    "int count = 0;\n" +
                    "for (entry in sorted) {\n" +
                    "    results.put(entry.key, entry.value)\n" +
                    "    if (count++ == 100) break\n" +
                    "}\n" +
                    "\n" +
                    "String output = \"CPU Accumulated          Process Name\\n\"\n" +
                    "output +=       \"-------------------------------------\\n\"\n" +
                    "for (entry in results) {\n" +
                    "    output += (entry.value as int) + \"  \\t---->\\t \" + entry.key + \"\\n\"\n" +
                    "}\n" +
                    "\n" +
                    "// context.set(\".out\", results.sort { -it.value })\n" +
                    "context.set(\".out\", output)");
            this.functions.store("rca", "ps-rss.groovy", "// usage: ps-rss.groovy?start=<timestamp>&end=<timestamp>&tenant=<pod>\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Cantor\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import com.salesforce.cantor.grpc.CantorOnGrpc\n" +
                    "\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = \"maiev-tenant-\" + getRequiredParameter(parameters, \"tenant\")\n" +
                    "\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "\n" +
                    "Set<String> instances = cx.cantor.events().metadata(namespace, \"instance-id\", start, end, null, null)\n" +
                    "context.set(\"groups\", instances)\n" +
                    "context.set(\"dimensions\", [\"core-rss\", \"caas-rss-7501\", \"caas-rss-7504\"])\n" +
                    "for (String instance : instances) {\n" +
                    "    List<Events.Event> results = new ArrayList<>()\n" +
                    "\n" +
                    "    // get events with compressed payloads\n" +
                    "    List<Events.Event> compressedPayloads = cx.cantor.events().get(namespace, start, end, [\"name\":\"ps\", \"instance-id\": instance as String], null, true)\n" +
                    "    List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "    // go through events one by one and decompress the payload\n" +
                    "    for (Events.Event e : compressedPayloads) {\n" +
                    "        events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "    }\n" +
                    "    compressedPayloads.clear()\n" +
                    "    compressedPayloads = null\n" +
                    "\n" +
                    "    for (Events.Event e : events) {\n" +
                    "        String payload = new String(e.payload)\n" +
                    "        List lines = payload.split(\"\\n\")\n" +
                    "        Map dimensions = new HashMap()\n" +
                    "        for (String line : lines) {\n" +
                    "            String[] words = line.trim().replaceAll(\"\\\\s+\", \" \").split(\" \")\n" +
                    "            if (!words[0].matches(\"-?\\\\d+(\\\\.\\\\d+)?\")) continue\n" +
                    "            double rss = words[5] as double\n" +
                    "            if (line.matches(\".*/home/sfdc/tmp.*\")) {\n" +
                    "                dimensions.put(\"core-rss\", rss)\n" +
                    "            } else if (line.matches(\".*server.port=7501.*\")) {\n" +
                    "                dimensions.put(\"caas-rss-7501\", rss)\n" +
                    "            } else if (line.matches(\".*server.port=7504.*\")) {\n" +
                    "                dimensions.put(\"caas-rss-7504\", rss)\n" +
                    "            } else {\n" +
                    "                continue\n" +
                    "            }\n" +
                    "        }\n" +
                    "        results.add(new Events.Event(e.timestampMillis, null, dimensions))\n" +
                    "    }\n" +
                    "\n" +
                    "    context.set(instance, results)\n" +
                    "}");
            this.functions.store("rca", "events.groovy", "// call like this:\n" +
                    "// http://localhost:8083/api/functions/run/maiev/events.groovy?start=1585061646768&end=1585061746768&namespace=maiev-tenant-cantor-prd&mq=[\"name\":\"~(iostat|df)\"]&out=results\n" +
                    "\n" +
                    "import com.google.common.io.ByteStreams\n" +
                    "import com.salesforce.cantor.Cantor\n" +
                    "import com.salesforce.cantor.Events\n" +
                    "import com.salesforce.cantor.functions.Context\n" +
                    "import com.salesforce.cantor.grpc.CantorOnGrpc\n" +
                    "\n" +
                    "import java.util.concurrent.TimeUnit\n" +
                    "import java.util.zip.GZIPInputStream\n" +
                    "\n" +
                    "static byte[] decompress(final byte[] compressedBytes) throws IOException {\n" +
                    "    if (compressedBytes == null || compressedBytes.length == 0) {\n" +
                    "        return null\n" +
                    "    }\n" +
                    "    return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(compressedBytes)))\n" +
                    "}\n" +
                    "\n" +
                    "static String getRequiredParameter(Map params, String name) {\n" +
                    "    if (params.get(name) == null) {\n" +
                    "        throw new IllegalArgumentException(\"missing parameter '\" + name + \"'\")\n" +
                    "    }\n" +
                    "    return params.get(name) as String\n" +
                    "}\n" +
                    "\n" +
                    "// speccial parameter 'context' is injected by the function executor\n" +
                    "Context cx = context\n" +
                    "// speccial parameter 'params' is injected by the function executor\n" +
                    "Map parameters = params\n" +
                    "// to access cantor\n" +
                    "\n" +
                    "// cantor namespace to run query against\n" +
                    "String namespace = getRequiredParameter(parameters, \"namespace\")\n" +
                    "\n" +
                    "// metadata query; example: mq=[\"name\":\"~(df|iostat)\",\"host\":\"~gs0.*\"]\n" +
                    "Map metadataQuery = null\n" +
                    "String mq = parameters.get(\"mq\") as String\n" +
                    "if (mq != null) {\n" +
                    "    metadataQuery = evaluate(mq) as Map\n" +
                    "}\n" +
                    "\n" +
                    "// dimensions query; example: dq=[\"available-mem-bytes\":\">100\"]\n" +
                    "Map dimensionsQuery = null\n" +
                    "String dq = parameters.get(\"dq\") as String\n" +
                    "if (dq != null) {\n" +
                    "    dimensionsQuery = evaluate(dq) as Map\n" +
                    "}\n" +
                    "\n" +
                    "// start and end timestamps in milliseconds\n" +
                    "long start = getRequiredParameter(parameters, \"start\") as long\n" +
                    "long end = getRequiredParameter(parameters, \"end\") as long\n" +
                    "\n" +
                    "// name of the context parameter to hold the results\n" +
                    "String outKey = getRequiredParameter(parameters, \"out\")\n" +
                    "\n" +
                    "// get events with compressed payloads\n" +
                    "List<Events.Event> compressedPayloads = cx.cantor.events().get(namespace, start, end, metadataQuery, dimensionsQuery, true)\n" +
                    "List<Events.Event> events = new ArrayList<>(compressedPayloads.size())\n" +
                    "// go through events one by one and decompress the payload\n" +
                    "for (Events.Event e : compressedPayloads) {\n" +
                    "    events.add(new Events.Event(e.timestampMillis, e.metadata, e.dimensions, decompress(e.payload)))\n" +
                    "}\n" +
                    "\n" +
                    "// results is maintained in the context with key name provided by 'out' parameter\n" +
                    "cx.set(outKey, events);\n" +
                    "\n" +
                    "// just to debug\n" +
                    "// cx.set(\".out\", events.get(0).payload)");
        } catch (IOException e) {
            throw new RuntimeException("Storing functors for RCA failed");
        }
    }

    @PUT
    @Path("/{namespace}")
    @Operation(summary = "Create a new function namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Function namespace was created or already exists"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response createNamespace(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.functions.create(namespace);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}")
    @Operation(summary = "Drop a function namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Function namespace was dropped or didn't exist"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response dropNamespace(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.functions.drop(namespace);
        return Response.ok().build();
    }

    @GET
    @Path("/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of all functions in the given namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all functions in the namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunctions(@PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request for all objects namespaces");
        return Response.ok(parser.toJson(this.functions.list(namespace))).build();
    }

    @GET
    @Path("/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the function with the given name",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunction(@PathParam("namespace") final String namespace,
                                @PathParam("function") final String functionName) throws IOException {
        final byte[] bytes = this.functions.get(namespace, functionName);
        if (bytes == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(bytes).build();
    }

    @GET
    @Path("/run/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute 'get' method on a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("namespace") final String namespace,
                                       @PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with get method", namespace, function);
        return doExecute(namespace, function, request, response);
    }

    @PUT
    @Path("/run/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute put method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response putExecuteFunction(@PathParam("namespace") final String namespace,
                                       @PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with put method", namespace, function);
        return doExecute(namespace, function, request, response);
    }

    @POST
    @Path("/run/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute post method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response postExecuteFunction(@PathParam("namespace") final String namespace,
                                        @PathParam("function") final String function,
                                        @Context final HttpServletRequest request,
                                        @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with post method", namespace, function);
        return doExecute(namespace, function, request, response);
    }

    @DELETE
    @Path("/run/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute delete method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteExecuteFunction(@PathParam("namespace") final String namespace,
                                          @PathParam("function") final String function,
                                          @Context final HttpServletRequest request,
                                          @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with delete method", namespace, function);
        return doExecute(namespace, function, request, response);
    }

    @PUT
    @Path("/{namespace}/{function}")
    @Operation(summary = "Store a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Function stored"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Namespace") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Function identifier") @PathParam("function") final String functionName,
                           final String body) {
        try {
            this.functions.store(namespace, functionName, body);
            return Response.status(Response.Status.CREATED).build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    @DELETE
    @Path("/{namespace}/{function}")
    @Operation(summary = "Remove a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Function removed"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace") @PathParam("namespace") final String namespace,
                         @Parameter(description = "Namespace identifier") @PathParam("function") final String function) {
        try {
            this.functions.delete(namespace, function);
            return Response.ok().build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    private Response doExecute(final String namespace,
                               final String function,
                               final HttpServletRequest request,
                               final HttpServletResponse response) {
        try {
            final com.salesforce.cantor.functions.Context context =
                    new com.salesforce.cantor.functions.Context(this.cantor, this.functions);
            // special parameters, http.request and http.response are passed to functions
            context.set("http.request", request);
            context.set("http.response", response);
            this.functions.run(namespace, function, context, getParams(request));

            // retrieve special parameter http.status from context
            final Object statusObject = context.get("http.status");
            final int status;
            if (statusObject instanceof String) {
                status = Integer.parseInt((String) statusObject);
            } else if (statusObject instanceof BigDecimal) {
                status = ((BigDecimal) statusObject).intValue();
            } else if (statusObject instanceof Long) {
                status = ((Long) statusObject).intValue();
            } else if (statusObject instanceof Integer) {
                status = (int) statusObject;
            } else {
                status = Response.Status.OK.getStatusCode();
            }
            final Response.ResponseBuilder builder = Response.status(status);
            // retrieve special parameter .out from context
            if (context.get(".out") != null) {
                builder.entity(context.get(".out"));
            }
            // retrieve special parameter http.headers from context
            if (context.get("http.headers") != null) {
                for (final Map.Entry<String, Object> header : ((Map<String, Object>) context.get("http.headers")).entrySet()) {
                    builder.header(header.getKey(), header.getValue());
                }
            }
            return builder.build();
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }
    }

    // convert http request query string parameters to a map of string to string
    private Map<String, String> getParams(final HttpServletRequest request) {
        final Map<String, String> params = new HashMap<>();
        for (final Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
            params.put(entry.getKey(), entry.getValue()[0]);
        }
        return params;
    }

    // convert throwable object stack trace to string
    private String toString(final Throwable throwable) {
        final StringWriter writer = new StringWriter();
        final PrintWriter printer = new PrintWriter(writer);
        throwable.printStackTrace(printer);
        return writer.toString();
    }
}
