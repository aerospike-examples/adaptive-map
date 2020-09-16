/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.storage.adaptive;

/**
 * Perform bit-wise operations (get, set) on an array of bytes. The array will grow as needed based on 
 * the operations performed, but never shrink smaller.
 * @author Tim
 *
 */
public class BitwiseData {
	private byte[] data;
	
	public BitwiseData() {
		super();
		data = new byte[1];
	}
	
	public BitwiseData(final byte[] initialValue) {
		this.data = initialValue;
	}
	
	private byte[] ensureCapacity(byte[] data, int capacity) {
		int oldSize = data == null ? 0 : data.length;
		if (capacity > oldSize) {
			byte[] newBuffer = new byte[capacity];
			
			for (int i = 0; data != null && i < capacity; i++) {
				newBuffer[i] = (i < oldSize) ? data[i] : 0; 
			}
			return newBuffer;
		}
		else {
			return data;
		}
	}
	
	/**
	 * Determine if a particular bit in the byte array is set. Returns true if set, false otherwise.
	 * <p>
	 * This operation will never change the underlying array.
	 * @param key - the byte array
	 * @param bit - the bit number to check, starting from 0
	 * @return true if the bit is set, false otherwise.
	 */
	protected boolean getBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);

		if (key == null || bite >= key.length) {
			return false;
		}
		return ((key[bite] >> bitInByte) & 0x1) > 0;
	}
	
	/**
	 * Set an appropriate bit in the underlying byte array. This method can grow the underlying array to accomodate the bit being inspected
	 * @param key - the byte array
	 * @param bit - the bit number to set, starting from 0
	 * @return An array with the bit set. This may be the original array or a copy of it if the array had to grow.
	 */
	protected byte[] setBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);
		byte[] data = ensureCapacity(key, bite+1);
		data[bite] |=  (1<< bitInByte);
		return data;
	}
	
	/**
	 * Clear a appropriate bit in the underlying byte array. This method can grow the underlying array to accomodate the bit being inspected
	 * @param key - the byte array
	 * @param bit - the bit number to clear, starting from 0
	 * @return An array with the bit set. This may be the original array or a copy of it if the array had to grow.
	 */
	protected byte[] clearBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);
		byte[] data = ensureCapacity(key, bite+1);
		data[bite] &=  ~(1<< bitInByte);
		return data;
	}
	
	public boolean get(int bitNum) {
		return this.getBit(this.data, bitNum);
	}

	public BitwiseData set(int bitNum, boolean value) {
		this.data = value ? setBit(this.data, bitNum) : clearBit(this.data, bitNum);
		return this;
	}
	
	public String toString(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < bytes.length * 8; i++) {
			sb.append(getBit(bytes, i) ? 1 : 0);
			if ((i%8) == 7) {
				sb.append(' ');
			}
		}
		return sb.toString();
	}
}	
