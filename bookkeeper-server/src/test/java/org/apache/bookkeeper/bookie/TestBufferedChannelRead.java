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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.hamcrest.CoreMatchers;

@RunWith(Parameterized.class)
public class TestBufferedChannelRead {

	private ByteBuf srcBuffer = generateEntryWithWrite(8);
	private int bufferedChannelCapacity;
	private ByteBuf dstBuffer;
	private long bufferedChannelPos;
	private int bufferedChannelLength;
	private int bufferedChannelUnpersistedBytesBound = 10;
	private Object testResult;
	
	@Mock
	private BufferedChannel bufferedChannel;
	private static FileChannel bufferedChannelFileChannel;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() {
		return Arrays.asList(new Object[][] {
			{0, null, -1, 0, 0},
			{0, generateEntryWithoutWrite(), 0, 0, 0},
			
			// Coverage
			{10, generateEntryWithoutWrite(), 0, 1, 8},
			{8, generateEntryWithoutWrite(), 1, 1, 7},
			{11, generateEntryWithoutWrite(), 0, 1, IOException.class},

			// Coverage 274 possibile mock, vedere slide. Stessa cosa 258
		});
	}
	
	@Rule 
	public MockitoRule rule = MockitoJUnit.rule();

	public TestBufferedChannelRead(int capacity, ByteBuf dst, long pos, int length,
			Object result){
		this.dstBuffer = dst;
		this.bufferedChannelPos = pos;
		this.bufferedChannelLength = length;
		this.bufferedChannelCapacity = capacity;
		this.testResult = result;
	}

	@Before
	public void beforeTest() throws IOException {

	
		ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
		File newLogFile = File.createTempFile("test", "log");
		newLogFile.deleteOnExit();
		bufferedChannelFileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

		bufferedChannel = new BufferedChannel(allocator, bufferedChannelFileChannel,
				bufferedChannelCapacity, bufferedChannelUnpersistedBytesBound);
		if (bufferedChannelCapacity == 11)
			srcBuffer = generateEntryWithWrite(0);
		bufferedChannel.write(srcBuffer);
	}
	
	@After
	public void close() throws IOException {
		bufferedChannelFileChannel.close();
	}
	
	@Test
	public void testRead() {
		try {
			 Assert.assertEquals(testResult, bufferedChannel.read(dstBuffer, bufferedChannelPos, bufferedChannelLength));		 
		} catch (Exception e) {
			Assert.assertEquals(testResult, e.getClass());
		}
	}

	private static ByteBuf generateEntryWithoutWrite() {
		return Unpooled.buffer(1024);
	}

	private ByteBuf generateEntryWithWrite(int length) {
		Random random = new Random();
		byte[] data = new byte[length];
		random.nextBytes(data);
		ByteBuf byteBuffer = Unpooled.buffer(1024);
		byteBuffer.writeLong(1);
		byteBuffer.writeLong(2);
		byteBuffer.writeLong(3);
		byteBuffer.writeLong(length);
		byteBuffer.resetWriterIndex();
		byteBuffer.writeBytes(data);
		return byteBuffer;
	}
}  


