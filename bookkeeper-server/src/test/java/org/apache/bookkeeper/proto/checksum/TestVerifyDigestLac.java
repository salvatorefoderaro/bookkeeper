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
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;

import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class TestVerifyDigestLac {

	private Object result;
	private static int length = 10;
	private int lacId;
	private ByteBufList received;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{-1, null, null},
			{0, generateLacWithDigest(0), (long)0},
			{1, generateLacWithDigest(0), (long)0}
		});
	}

	public TestVerifyDigestLac(int lacId, ByteBufList received, Object result){
		this.lacId = lacId;
		this.received = received;
		this.result = result;
	}


	@Test
	public void testRead() throws GeneralSecurityException {

		DigestManager digest = DigestManager.instantiate(1, "testPassword".getBytes(), DigestType.HMAC, UnpooledByteBufAllocator.DEFAULT, false);

			try {
				Assert.assertEquals(result, digest.verifyDigestAndReturnLac(received.getBuffer(0)));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				Assert.assertEquals(result, e.getMessage());
			}
	}
	

	private static ByteBufList generateLacWithDigest(int lacID) throws GeneralSecurityException {
		DigestManager digest = DigestManager.instantiate(1, "testPassword".getBytes(), DigestType.HMAC, UnpooledByteBufAllocator.DEFAULT, false);
		ByteBufList a = digest.computeDigestAndPackageForSendingLac(lacID);
		return a;
	}

	private static ByteBuf generateEntry(int length) {
		byte[] data = new byte[length];
		ByteBuf bb = Unpooled.buffer(length);
		bb.writeBytes(data);
		return bb;
	}

}  


