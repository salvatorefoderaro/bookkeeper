/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.proto.checksum;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.bookie.BufferedChannel;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class TestDigestManagerComputeDigestData {

	private ByteBuf data;
	private long lastAddConfirmed;
	private static long ledgerId;
	private static long entryId;
	private long length;
	private static long lac;
	
	private DigestType type;
	private Object result;
	private DigestManager digestManager;
	private ByteBuf testEntry;
	private boolean useV2Protocol;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			
			// Suite minimale
			{null, -1, -1, 0, DigestType.HMAC,true, NullPointerException.class},
			{generateEntry(1), 1, 2, 1, DigestType.CRC32, true, 0},
			{generateEntry(0), 0, 2, 0, DigestType.CRC32C, false, 0},
			{generateEntry(1), 1, 2, 1, DigestType.DUMMY, true, 0}

		});
	}
	
	@Rule 
	public MockitoRule rule = MockitoJUnit.rule();

	public TestDigestManagerComputeDigestData(ByteBuf data, long lastAddConfirmed, long entryId, long length, DigestType type, boolean useV2Protocol, Object result){
		this.data = data;
		this.lastAddConfirmed = lastAddConfirmed;
		this.entryId = entryId;
		this.length = length;
		this.type = type;
		this.result = result;
		this.useV2Protocol = useV2Protocol;
	}

	@Before
	public void setUp() throws GeneralSecurityException {
			digestManager = DigestManager.instantiate(1, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, useV2Protocol);

		testEntry = generateEntry((int)length);
	}

	@Test
	public void testComputeDigestData() {

		try {
			// Assert that the buffer of data contained in the byteBuf is equal to what sent
			ByteBufList byteBuf = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, data);
			Assert.assertEquals(testEntry.readLong(), byteBuf.getBuffer(1).readLong());
			
		} catch (Exception e) {
			Assert.assertEquals(result, e.getClass());
		}
	}

	private static ByteBuf generateEntry(int length) {
		byte[] data = new byte[length];
		ByteBuf byteBuffer = Unpooled.buffer(1024);
		byteBuffer.writeLong(ledgerId); // Ledger
		byteBuffer.writeLong(entryId); // Entry
		byteBuffer.writeLong(lac); // LAC
		byteBuffer.writeLong(length); // Length
		byteBuffer.writeBytes(data);
		return byteBuffer;
	}
	
	private static ByteBuf generateBadEntry(int length) {
		byte[] data = new byte[length];
		ByteBuf byteBuffer = Unpooled.buffer(1024);
		byteBuffer.writeBytes(data);
		return byteBuffer;
	}

}  


