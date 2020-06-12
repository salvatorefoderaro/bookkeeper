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
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.stats.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
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
public class TestDigestManagerRecoveryData {

	private ByteBufList data;
	private static long ledgerId;
	private static long entryId;
	private static long length;
	private static long lac;
	
	private DigestType type;
	private Object result;
	private DigestManager test;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{null, -1, 0, DigestType.HMAC, NullPointerException.class},
			{generateLastAddConfirmed(1, DigestType.CRC32, 1, true), 2, 1, DigestType.CRC32, BKDigestMatchException.class},
			{generateLastAddConfirmed(-1, DigestType.CRC32, 1, true), 2, 0, DigestType.CRC32C, BKDigestMatchException.class},
			{generateLastAddConfirmed(-1, DigestType.CRC32, 1, true), 2, 3, DigestType.DUMMY, (long)1}

		});
	}
	
	@Rule 
	public MockitoRule rule = MockitoJUnit.rule();

	public TestDigestManagerRecoveryData(ByteBufList data, long entryId, long length, DigestType type, Object result){
		this.data = data;
		this.length = length;
		this.type = type;
		this.result = result;
	}

	@Before
	public void setUp() throws GeneralSecurityException {
		if (type == DigestType.CRC32)
			test = DigestManager.instantiate(1, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, false);
		else
			test = DigestManager.instantiate(1, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, true);

	}

	@Test
	public void testRead() {

		try {
			RecoveryData a = test.verifyDigestAndReturnLastConfirmed(data.getBuffer(0));
			Assert.assertEquals(result, a.getLastAddConfirmed());
			Assert.assertEquals(0, a.getLength());
		} catch (Exception e) {
			Assert.assertEquals(result, e.getClass());
		}
	}
	
	private static ByteBufList generateLastAddConfirmed(int lacID, DigestType digestType, long ledgerID, boolean useV2Protocol) throws GeneralSecurityException {
		DigestManager digest = DigestManager.instantiate(ledgerID, "testPassword".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, useV2Protocol);
		ByteBufList a = digest.computeDigestAndPackageForSending(entryId, 1, length, generateEntry(20));
		return a;
	}
	
	private static ByteBuf generateEntry(int length) {
		byte[] data = new byte[length];
		ByteBuf bb = Unpooled.buffer(1024);
		bb.writeLong(ledgerId); // Ledger
		bb.writeLong(entryId); // Entry
		bb.writeLong(lac); // LAC
		bb.writeLong(length); // Length
		bb.writeBytes(data);
		return bb;
	}



}  


