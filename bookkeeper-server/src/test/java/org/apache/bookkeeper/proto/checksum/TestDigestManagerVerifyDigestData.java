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
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.stats.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class TestDigestManagerVerifyDigestData {

	private Object result;
	private static int length = 1;
	private int entryId;
	private int ledgerId;
	private DigestType type;
	private ByteBufList receivedData;
	private DigestManager digestManager;
	private ByteBuf mineByteBuf;
	
	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{0, 0, DigestType.HMAC,  generateDataWithDigest(0, 1, DigestType.HMAC), BKDigestMatchException.class},
			{-1, 1,  DigestType.DUMMY, generateDataWithDigest(1, 1, DigestType.DUMMY), BKDigestMatchException.class},
			{1, 1,  DigestType.CRC32, generateDataWithDigest(1, 1, DigestType.HMAC), BKDigestMatchException.class},
			{1, 1,  DigestType.CRC32C, generateDataWithDigest(1, 1, DigestType.HMAC), BKDigestMatchException.class},
			
			// Coverage
			{1, 1,  DigestType.CRC32C, generateBadDataWithDigest(1, 1, DigestType.HMAC), BKDigestMatchException.class},
			{1, 1,  DigestType.CRC32, generateDataWithDigest(1, 1, DigestType.CRC32), 0},
			{1, 1,  DigestType.CRC32, generateDataWithDigest(1, 0, DigestType.CRC32), BKDigestMatchException.class}
			// Coverage
		});
	}

	public TestDigestManagerVerifyDigestData(int ledgerId, int entryId, DigestType type, ByteBufList received, Object result){
		this.entryId = entryId;
		this.ledgerId = ledgerId;
		this.type = type;
		this.receivedData = received;
		this.result = result;
	}


	@Before
	public void beforeTest() throws GeneralSecurityException {

		digestManager = DigestManager.instantiate(ledgerId, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, false);
		
		mineByteBuf = generateEntry(length);

	}
	
	@Test
	public void testRead() throws GeneralSecurityException{

			Assert.assertEquals(mineByteBuf, receivedData.getBuffer(1));
			try {
				Assert.assertEquals(mineByteBuf, digestManager.verifyDigestAndReturnData(entryId, receivedData.coalesce(receivedData)));
			} catch (BKDigestMatchException e) {
				Assert.assertEquals(result, e.getClass());
			}
	}

	private static ByteBufList generateDataWithDigest(int receivedLedgerId, int receivedEntryId, DigestType receivedType) throws GeneralSecurityException {
		DigestManager digest = DigestManager.instantiate(receivedLedgerId, "testPassword".getBytes(), receivedType, UnpooledByteBufAllocator.DEFAULT, false);
		ByteBuf test1 = generateEntry(length);
		ByteBufList a = digest.computeDigestAndPackageForSending(receivedEntryId, 0,  length, test1);
		return a;
	}
	
	private static ByteBufList generateBadDataWithDigest(int receivedLedgerId, int receivedEntryId, DigestType receivedType) throws GeneralSecurityException {
		ByteBuf test1 = generateEntry(length);
		ByteBuf badHeader = Unpooled.buffer(length);
		badHeader.writeLong(10);
		return ByteBufList.get(badHeader, test1);
	}

	private static ByteBuf generateEntry(int length) {
		byte[] data = new byte[length];
		ByteBuf bb = Unpooled.buffer(length);
		bb.writeBytes(data);
		return bb;
	}

}  


