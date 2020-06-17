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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;

@RunWith(Parameterized.class)
public class TestBufferedChannelWrite {

	private ByteBuf src;
	private int writeCapacity = 40;
	private long unpersistedBytesBound;
	private Object result;
	
	private BufferedChannel bufferedChannel;
	private final static long HEADER_SIZE = 32L;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{ null, 0, (long)0},
			{generateEntryWithWrite(0), 1, 0L},
			{generateEntryWithWrite(1), 1, 1L + HEADER_SIZE},
			
			 // Coverage
			{generateEntryWithWrite(1), -33, (long)0},
			
			// Mutante 89
			{generateEntryWithWrite(1), -32, (long)0},
			{generateEntryWithWrite(12), -33, (long)40},
			{generateEntryWithWrite(12), 12, (long)44}

			});		
		
	}

	public TestBufferedChannelWrite(ByteBuf src, 
			int unpersistedBytesBound, Object result){
		this.src = src;
		this.unpersistedBytesBound = unpersistedBytesBound+HEADER_SIZE;
		this.result = result;
	}

	@Before
	public void beforeTest() throws Exception {
		bufferedChannel = createObject(writeCapacity, unpersistedBytesBound);
	}
	
	@After
	public void close() throws IOException {
		bufferedChannel.close();
	}

	@Test
	public void testWrite() throws Exception {

			try {
				bufferedChannel.write(src);
				Assert.assertEquals((long)result, bufferedChannel.fileChannel.size());
			} catch (Exception e){
				Assert.assertEquals(result, bufferedChannel.fileChannel.size());
		}
	}

	public static BufferedChannel createObject(int capacity,  long unpersistedBytesBound) throws Exception {

		ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
		File newLogFile = File.createTempFile("test", "log");
		newLogFile.deleteOnExit();
		FileChannel fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

		BufferedChannel bufferedChannel = new BufferedChannel(allocator, fileChannel,
				capacity, unpersistedBytesBound);

		return bufferedChannel;
	}

	private static ByteBuf generateEntryWithWrite(int length) {
		Random random = new Random();
		byte[] data = new byte[length];
		random.nextBytes(data);
		ByteBuf byteBuffer = Unpooled.buffer(1024);
		byteBuffer.writeLong(0);
		byteBuffer.writeLong(1);
		byteBuffer.writeLong(2);
		byteBuffer.writeLong(length);
		byteBuffer.writeBytes(data);
		return byteBuffer;
	}

}  


