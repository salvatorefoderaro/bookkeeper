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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
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
public class TestBufferedDataWrite {

	private ByteBuf src = generateEntryWithWrite(1);
	private int writeCapacity;
	private ByteBuf dst;
	private long pos;
	private int length;
	private int unpersistedBytesBound = 0;
	private Object result;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			{-1, null, -2, -1, Exception.class},
			{0, generateEntryWithoutWrite(0), 0, 1, 0},
			{1, generateEntryWithoutWrite(1), 2, 2, Exception.class}
		});
	}

	public TestBufferedDataWrite(int numOfWrites, ByteBuf dst, long pos, int length,
			Object result){
		this.dst = dst;
		this.pos = pos;
		this.length = length;
		this.writeCapacity = numOfWrites;
		this.result = result;
	}

	@Rule
	public ExpectedException exceptions = ExpectedException.none();

	@Test
	public void testRead() throws Exception {

		if (!(result instanceof Integer)) {
			exceptions.expect(Exception.class);
		}

		BufferedChannel bufferedChannel = createObject(writeCapacity, unpersistedBytesBound);

		src.markReaderIndex();
		src.markWriterIndex();
		src.writeBytes("T".getBytes());
		System.out.println(length);

		bufferedChannel.write(src);
		System.out.println(length);

		src.resetReaderIndex();
		src.resetWriterIndex();
		if ((result instanceof Integer)) {

			Assert.assertEquals(result, bufferedChannel.read(dst, pos, length));
		}

	}

	public static BufferedChannel createObject(int writeCapacity,  int unpersistedBytesBound) throws Exception {

		ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
		File newLogFile = File.createTempFile("test", "log");
		newLogFile.deleteOnExit();
		FileChannel fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

		BufferedChannel logChannel = new BufferedChannel(allocator, fileChannel,
				writeCapacity, 10, unpersistedBytesBound);

		return logChannel;
	}

	private static ByteBuf generateEntryWithoutWrite(int length) {
		return Unpooled.buffer(length);
	}

	private static ByteBuf generateEntryWithWrite(int length) {
		Random random = new Random();
		byte[] data = new byte[length];
		random.nextBytes(data);
		ByteBuf bb = Unpooled.buffer(length);
		bb.writeBytes(data);
		return bb;
	}
}  

