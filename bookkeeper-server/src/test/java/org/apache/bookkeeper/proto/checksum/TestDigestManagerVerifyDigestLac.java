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

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class TestDigestManagerVerifyDigestLac {

	private Object result;
	private static int length = 10;
	private ByteBufList received;
	private DigestManager digest;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{null, NullPointerException.class},
			{generateLacWithDigest(-1, DigestType.CRC32, 1, true), (long)-1},
			
			{generateLacWithDigest(0, DigestType.HMAC,1, false), BKDigestMatchException.class},
			{generateLacWithDigest(1, DigestType.CRC32C, 1, true), BKDigestMatchException.class},
			
			/*// Coverage
			{generateBadLacWithDigest(1), BKDigestMatchException.class},
			{generateLacWithDigest(-1, DigestType.CRC32, 0, true), BKDigestMatchException.class},

			// Mutation
			{generateLacWithDigest(0, DigestType.HMAC,1, true), BKDigestMatchException.class},
*/
		});
	}

	public TestDigestManagerVerifyDigestLac(ByteBufList received, Object result){
		this.received = received;
		this.result = result;
	}

	@Before
	public void makeDigestManager() throws GeneralSecurityException {
		digest = DigestManager.instantiate(1, "testPassword".getBytes(), DigestType.CRC32, UnpooledByteBufAllocator.DEFAULT, false);
	}

	@Test
	public void testRead() throws GeneralSecurityException {

		try {
			Assert.assertEquals(result, digest.verifyDigestAndReturnLac(received.getBuffer(0)));
		} catch (Exception e) {
			Assert.assertEquals(result, e.getClass());
		}
	}
	
	private static ByteBufList generateLacWithDigest(int lacID, DigestType digestType, long ledgerID, boolean useV2Protocol) throws GeneralSecurityException {
		DigestManager digest = DigestManager.instantiate(ledgerID, "testPassword".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, useV2Protocol);
		ByteBufList a = digest.computeDigestAndPackageForSendingLac(lacID);
		return a;
	}

	private static ByteBufList generateBadLacWithDigest(int lacID) throws GeneralSecurityException {
		ByteBuf badHeader = Unpooled.buffer(length);
		badHeader.writeLong(lacID);

		ByteBuf test1 = Unpooled.buffer(length);
		
		byte[] data = new byte[length];
		test1.writeBytes(data);
		return ByteBufList.get(badHeader, test1);
	}
	
}  


